using Microsoft.Extensions.Logging;
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
        private readonly ILogger<ElasticsearchStorage> _logger;

        public ElasticsearchStorage(IOptions<ElasticsearchOptions> options, ILogger<ElasticsearchStorage> logger)
        {
            _logger = logger;
            _httpClient = new HttpClient();
            _httpClient.BaseAddress = new Uri(options.Value.Url);
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            _indexName = options.Value.Index;

            _logger.LogInformation("ElasticsearchStorage initialized with Url={Url}, Index={Index}", options.Value.Url, _indexName);
        }

        public async Task StoreAsync(LogEvent logEvent)
        {
            try
            {
                var json = JsonSerializer.Serialize(logEvent, _jsonOptions);
                _logger.LogDebug("Serialized event to JSON: {Json}", json.Length > 500 ? json.Substring(0, 500) + "..." : json);

                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync($"{_indexName}/_doc", content);

                if (!response.IsSuccessStatusCode)
                {
                    var errorBody = await response.Content.ReadAsStringAsync();
                    _logger.LogError("Elasticsearch error: StatusCode={StatusCode}, Error={Error}", response.StatusCode, errorBody);
                    throw new Exception($"Elasticsearch error: {response.StatusCode} - {errorBody}");
                }

                _logger.LogInformation("Document indexed successfully in {Index}.", _indexName);
            }
            catch (HttpRequestException ex)
            {
                _logger.LogError(ex, "HTTP request to Elasticsearch failed.");
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while storing event in Elasticsearch.");
                throw;
            }
        }
    }

    public class ElasticsearchOptions
    {
        public string Url { get; set; }
        public string Index { get; set; }
    }
}
