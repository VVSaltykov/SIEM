using Microsoft.Extensions.Options;
using Nest;
using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Core.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace SIEM.LogCollector.Infrastructure.Storages
{
    public class ElasticsearchStorage : ILogStorage
    {
        private readonly HttpClient _httpClient;
        private readonly string _indexName;
        private readonly JsonSerializerOptions _jsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        public ElasticsearchStorage(IOptions<ElasticsearchOptions> options)
        {
            _httpClient = new HttpClient();
            _httpClient.BaseAddress = new Uri(options.Value.Url);
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            // Если нужно, можно добавить совместимость с версией 8 (обычно не требуется)
            // _httpClient.DefaultRequestHeaders.Add("Accept", "application/vnd.elasticsearch+json;compatible-with=8");
            _indexName = options.Value.Index;
        }

        public async Task StoreAsync(LogEvent logEvent)
        {
            var json = JsonSerializer.Serialize(logEvent, _jsonOptions);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var response = await _httpClient.PostAsync($"{_indexName}/_doc", content);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync();
                throw new Exception($"Elasticsearch error: {response.StatusCode} - {errorBody}");
            }
        }
    }

    public class ElasticsearchOptions
    {
        public string Url { get; set; } = "http://localhost:9200";
        public string Index { get; set; } = "kaspersky-logs";
    }
}
