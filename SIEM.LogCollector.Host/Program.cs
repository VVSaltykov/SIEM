using LogCollector.Infrastructure.Receivers;
using Serilog;
using SIEM.LogCollector.Core.Interfaces;
using SIEM.LogCollector.Infrastructure.Consumers;
using SIEM.LogCollector.Infrastructure.Producers;
using SIEM.LogCollector.Infrastructure.Receivers;
using SIEM.LogCollector.Infrastructure.Storages;

namespace SIEM.LogCollector.Host
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateBootstrapLogger();

            try
            {
                Log.Information("Starting LogCollector...");
                CreateHostBuilder(args).Build().Run();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "LogCollector terminated unexpectedly");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder(args)
                .UseSerilog((context, config) =>
                    config.ReadFrom.Configuration(context.Configuration))
                .ConfigureServices((context, services) =>
                {
                    services.Configure<ReceiverOptions>(context.Configuration.GetSection("Receiver"));
                    services.Configure<KafkaOptions>(context.Configuration.GetSection("Kafka"));
                    services.Configure<FilterOptions>(context.Configuration.GetSection("Filter"));

                    services.AddSingleton<ILogParser, SyslogParser>();
                    services.AddSingleton<ILogProcessor, EnrichmentProcessor>();
                    services.AddSingleton<ILogProcessor, FilterProcessor>();
                    services.AddSingleton<ILogStorage, ElasticsearchStorage>();
                    services.AddHostedService<KafkaConsumer>();
                });
    }
}