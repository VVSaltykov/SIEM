using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Core.Models
{
    public class LogEvent
    {
        public DateTime Timestamp { get; set; }
        public string? Host { get; set; }
        public string? Facility { get; set; }
        public string? Severity { get; set; }
        public string? Message { get; set; }
        public Dictionary<string, object> ExtractedFields { get; set; } = new();
        public Dictionary<string, object> Metadata { get; set; } = new();
    }
}
