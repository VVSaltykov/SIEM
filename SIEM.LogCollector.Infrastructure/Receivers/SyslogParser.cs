using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Infrastructure.Receivers
{
    public partial class SyslogParser : ILogParser
    {
        private static readonly Regex _rfc5424Regex = new(
            @"^<(?<priority>\d+)>\d+ (?<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z) (?<host>\S+) (?<program>\S+) (?<message>.*)$",
            RegexOptions.Compiled);

        private static readonly Regex _rfc3164Regex = new(
            @"^<(?<priority>\d+)>(?<timestamp>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}) (?<host>\S+) (?<program>\S+): (?<message>.*)$",
            RegexOptions.Compiled);

        public bool CanParse(string rawData)
        {
            return _rfc5424Regex.IsMatch(rawData) || _rfc3164Regex.IsMatch(rawData);
        }

        public LogEvent Parse(string rawData)
        {
            var match = _rfc5424Regex.Match(rawData);
            if (!match.Success)
                match = _rfc3164Regex.Match(rawData);

            if (!match.Success)
                throw new ArgumentException("Invalid syslog format");

            var priority = int.Parse(match.Groups["priority"].Value);
            var facility = (priority / 8).ToString();
            var severity = (priority % 8).ToString();

            var timestamp = match.Groups["timestamp"].Value;
            var parsedTimestamp = ParseTimestamp(timestamp);

            return new LogEvent
            {
                Timestamp = parsedTimestamp,
                Host = match.Groups["host"].Value,
                Facility = facility,
                Severity = severity,
                Message = match.Groups["message"].Value,
                ExtractedFields = new Dictionary<string, object>
                {
                    ["priority"] = priority,
                    ["program"] = match.Groups["program"].Value,
                    ["raw_timestamp"] = timestamp
                }
            };
        }

        private DateTime ParseTimestamp(string timestamp)
        {
            if (DateTime.TryParseExact(timestamp, "yyyy-MM-ddTHH:mm:ss.fffZ",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal,
                out var dt))
                return dt.ToUniversalTime();

            if (DateTime.TryParseExact(timestamp, "MMM dd HH:mm:ss",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeLocal,
                out dt))
                return dt.ToUniversalTime();

            return DateTime.UtcNow;
        }
    }
}
