using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Shared.Extensions
{
    public static class StringExtensions
    {
        public static string Scrub(this string input)
        {
            if (string.IsNullOrEmpty(input)) return input;
            // Удаляем управляющие символы, оставляем только печатные
            return new string(input.Where(c => !char.IsControl(c)).ToArray());
        }
    }
}
