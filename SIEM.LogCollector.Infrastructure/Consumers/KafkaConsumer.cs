using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Infrastructure.Consumers
{
    public class KafkaConsumer : BackgroundService
    {
        private readonly ILogger<KafkaConsumer> _logger;
        private readonly IConsumer<string, string> _consumer;
        private readonly string _topic;
        private readonly IEnumerable<ILogProcessor> _processors;
        private readonly ILogStorage _storage;
        private readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNameCaseInsensitive = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public KafkaConsumer(
            ILogger<KafkaConsumer> logger,
            IOptions<KafkaConsumerOptions> options,
            IEnumerable<ILogProcessor> processors,
            ILogStorage storage)
        {
            _logger = logger;
            _processors = processors;
            _storage = storage;

            _logger.LogInformation("Initializing KafkaConsumer with BootstrapServers={BootstrapServers}, Topic={Topic}, GroupId={GroupId}",
                options.Value.BootstrapServers, options.Value.Topic, options.Value.GroupId);

            var config = new ConsumerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
                GroupId = options.Value.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                AllowAutoCreateTopics = true
            };

            try
            {
                _consumer = new ConsumerBuilder<string, string>(config).Build();
                _topic = options.Value.Topic;
                _logger.LogInformation("Kafka consumer instance created successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to create Kafka consumer.");
                throw;
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                _consumer.Subscribe(_topic);
                _logger.LogInformation("Kafka consumer started. Subscribed to topic: {Topic}", _topic);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to subscribe to topic {Topic}", _topic);
                throw;
            }

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string> consumeResult;
                    try
                    {
                        consumeResult = _consumer.Consume(stoppingToken);
                        if (consumeResult?.Message?.Value == null)
                            continue;
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogInformation("Consume operation cancelled.");
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error while consuming message from Kafka.");
                        continue;
                    }

                    _logger.LogInformation("Received message from Kafka. Partition: {Partition}, Offset: {Offset}, Key: {Key}",
                        consumeResult.Partition, consumeResult.Offset, consumeResult.Message.Key);

                    try
                    {
                        var logEvent = JsonSerializer.Deserialize<LogEvent>(
                            consumeResult.Message.Value,
                            _jsonOptions);

                        if (logEvent == null)
                        {
                            _logger.LogWarning("Failed to deserialize message (null result). Raw value: {Value}",
                                consumeResult.Message.Value.Length > 200 ? consumeResult.Message.Value.Substring(0, 200) + "..." : consumeResult.Message.Value);
                            continue;
                        }

                        _logger.LogInformation("Deserialized LogEvent: Host={Host}, Timestamp={Timestamp}, Message length={Length}",
                            logEvent.Host, logEvent.Timestamp, logEvent.Message?.Length ?? 0);

                        var processed = logEvent;
                        bool processedByAny = false;
                        foreach (var processor in _processors)
                        {
                            try
                            {
                                _logger.LogDebug("Processing with processor {ProcessorType}", processor.GetType().Name);
                                processed = await processor.ProcessAsync(processed);
                                if (processed == null)
                                {
                                    _logger.LogWarning("Processor {ProcessorType} returned null. Event will be dropped.", processor.GetType().Name);
                                    break;
                                }
                                processedByAny = true;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error in processor {ProcessorType}", processor.GetType().Name);
                                throw;
                            }
                        }

                        if (processed != null && processedByAny)
                        {
                            _logger.LogInformation("Event passed all processors. Saving to Elasticsearch...");
                            await _storage.StoreAsync(processed);
                            _logger.LogInformation("Event successfully saved to Elasticsearch.");
                        }
                        else if (processed == null)
                        {
                            _logger.LogWarning("Event was filtered out by a processor.");
                        }
                        else
                        {
                            _logger.LogWarning("No processors executed? processed={Processed}, processedByAny={ProcessedByAny}", processed != null, processedByAny);
                        }

                        _consumer.Commit(consumeResult);
                        _logger.LogDebug("Committed offset {Offset} for partition {Partition}", consumeResult.Offset, consumeResult.Partition);
                    }
                    catch (JsonException ex)
                    {
                        _logger.LogError(ex, "JSON deserialization error. Raw message: {Value}",
                            consumeResult.Message.Value.Length > 500 ? consumeResult.Message.Value.Substring(0, 500) + "..." : consumeResult.Message.Value);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message from Kafka (Partition={Partition}, Offset={Offset})",
                            consumeResult.Partition, consumeResult.Offset);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Consumer stopped due to cancellation.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in Kafka consumer loop.");
            }
            finally
            {
                _consumer.Close();
                _logger.LogInformation("Kafka consumer closed.");
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Stopping Kafka consumer...");
            _consumer.Close();
            await base.StopAsync(cancellationToken);
        }
    }

    public class KafkaConsumerOptions
    {
        public string BootstrapServers { get; set; }
        public string Topic { get; set; }
        public string GroupId { get; set; }
    }
}
