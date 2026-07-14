using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Infrastructure.Producers
{
    public class KafkaLogProducer : ILogProducer
    {
        private readonly IProducer<string, string> _producer;
        private readonly string _topic;
        private readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        public KafkaLogProducer(IOptions<KafkaOptions> options)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = options.Value.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageSendMaxRetries = 3,
                CompressionType = CompressionType.Snappy
            };
            _producer = new ProducerBuilder<string, string>(config).Build();
            _topic = options.Value.Topic;
        }

        public async Task ProduceAsync(LogEvent logEvent, CancellationToken cancellationToken = default)
        {
            var json = JsonSerializer.Serialize(logEvent, _jsonOptions);
            var message = new Message<string, string>
            {
                Key = logEvent.Host ?? "unknown",
                Value = json
            };
            await _producer.ProduceAsync(_topic, message, cancellationToken);
        }
    }

    public class KafkaOptions
    {
        public string BootstrapServers { get; set; } = "localhost:9093";
        public string Topic { get; set; } = "logs";
    }
}
