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
        private readonly IEnumerable<ILogProcessor> _processors; // если процессоры остались
        private readonly ILogStorage _storage; // вместо ILogProducer
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

            var config = new ConsumerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
                GroupId = options.Value.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                AllowAutoCreateTopics = true
            };

            _consumer = new ConsumerBuilder<string, string>(config).Build();
            _topic = options.Value.Topic;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(_topic);
            _logger.LogInformation("Kafka consumer started. Subscribed to {Topic}", _topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var consumeResult = _consumer.Consume(stoppingToken);
                    if (consumeResult?.Message?.Value == null)
                        continue;

                    try
                    {
                        var logEvent = JsonSerializer.Deserialize<LogEvent>(
                            consumeResult.Message.Value,
                            _jsonOptions);

                        if (logEvent == null)
                        {
                            _logger.LogWarning("Failed to deserialize message");
                            continue;
                        }

                        var processed = logEvent;
                        foreach (var processor in _processors)
                        {
                            processed = await processor.ProcessAsync(processed);
                            if (processed == null) break;
                        }

                        if (processed != null)
                            await _storage.StoreAsync(processed);

                        _consumer.Commit(consumeResult);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing message");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Consumer stopped");
            }
            finally
            {
                _consumer.Close();
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
