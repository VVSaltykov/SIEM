using Microsoft.Extensions.Options;
using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Infrastructure.Receivers
{
    public class FilterProcessor : ILogProcessor
    {
        private readonly FilterOptions _options;

        public FilterProcessor(IOptions<FilterOptions> options)
        {
            _options = options.Value;
        }

        public Task<LogEvent?> ProcessAsync(LogEvent logEvent)
        {
            // Пример: если сообщение содержит "ignore", отбрасываем
            if (!string.IsNullOrEmpty(logEvent.Message) && logEvent.Message.Contains("ignore", StringComparison.OrdinalIgnoreCase))
                return Task.FromResult<LogEvent?>(null);

            return Task.FromResult(logEvent)!;
        }
    }

    public class FilterOptions
    {
        public List<string> IgnoredPatterns { get; set; } = new();
    }
}
