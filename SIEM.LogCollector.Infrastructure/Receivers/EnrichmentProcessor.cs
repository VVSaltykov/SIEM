using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Infrastructure.Receivers
{
    public class EnrichmentProcessor : ILogProcessor
    {
        public Task<LogEvent?> ProcessAsync(LogEvent logEvent)
        {
            logEvent.Metadata["ReceivedAt"] = DateTime.UtcNow;
            logEvent.Metadata["Source"] = "syslog";
            return Task.FromResult(logEvent)!;
        }
    }
}
