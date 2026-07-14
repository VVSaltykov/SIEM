using SIEM.LogCollector.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Core.Interfaces
{
    public interface ILogProcessor
    {
        Task<LogEvent?> ProcessAsync(LogEvent logEvent);
    }
}
