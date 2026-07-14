using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Core.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SIEM.LogCollector.Core.Interfaces;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace LogCollector.Infrastructure.Receivers;

public class TcpSyslogReceiver : BackgroundService, ILogReceiver
{
    private readonly ILogger<TcpSyslogReceiver> _logger;
    private readonly IEnumerable<ILogParser> _parsers;
    private readonly ILogProcessor _processor;
    private readonly ILogProducer _producer;
    private readonly ReceiverOptions _options;
    private TcpListener? _listener;

    public TcpSyslogReceiver(
        ILogger<TcpSyslogReceiver> logger,
        IEnumerable<ILogParser> parsers,
        ILogProcessor processor,
        ILogProducer producer,
        IOptions<ReceiverOptions> options)
    {
        _logger = logger;
        _parsers = parsers;
        _processor = processor;
        _producer = producer;
        _options = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _listener = new TcpListener(IPAddress.Any, _options.Port);
        _listener.Start();
        _logger.LogInformation("TCP Syslog receiver started on port {Port}", _options.Port);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var client = await _listener.AcceptTcpClientAsync(stoppingToken);
                _ = HandleClientAsync(client, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error accepting TCP client");
            }
        }

        _listener.Stop();
        _logger.LogInformation("TCP Syslog receiver stopped");
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken token)
    {
        using var _ = client;
        try
        {
            var stream = client.GetStream();
            using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
            var buffer = new char[4096];
            int read;

            while ((read = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
            {
                var raw = new string(buffer, 0, read);
                await ProcessRawDataAsync(raw, token);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing client connection");
        }
    }

    private async Task ProcessRawDataAsync(string raw, CancellationToken token)
    {
        try
        {
            var parser = _parsers.FirstOrDefault(p => p.CanParse(raw));
            if (parser == null)
            {
                _logger.LogWarning("No parser found for raw data: {Raw}", raw);
                return;
            }

            var logEvent = parser.Parse(raw);
            if (logEvent == null)
                return;

            var processed = await _processor.ProcessAsync(logEvent);
            if (processed != null)
                await _producer.ProduceAsync(processed, token);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing raw data: {Raw}", raw);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken) => ExecuteAsync(cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

public class ReceiverOptions
{
    public int Port { get; set; } = 514;
}