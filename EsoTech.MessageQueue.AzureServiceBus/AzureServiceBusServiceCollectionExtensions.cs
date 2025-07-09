using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using System;

namespace EsoTech.MessageQueue.AzureServicebus
{
    public static class AzureServiceBusServiceCollectionExtensions
    {
        public static IServiceCollection AddAzureServiceBusMessageQueue(this IServiceCollection self, Action<AzureServiceBusConfiguration> configure) =>
            self.AddAzureServiceBusMessageQueue((options, _) => configure(options));
        public static IServiceCollection AddAzureServiceBusMessageQueue(this IServiceCollection self, Action<AzureServiceBusConfiguration, IServiceProvider>? configure = null)
        {
            self.TryAddSingleton<AzureServiceBusClientHolder>();
            self.TryAddSingleton<AzureServiceBusNamingConvention>();
            self.TryAddSingleton<HashFunction>();
            self.TryAddSingleton<IMessageQueue, AzureServiceBusMessageSender>();
            self.TryAddSingleton<IMessageConsumer, AzureServiceBusConsumer>();
            self.TryAddSingleton<AzureServiceBusManager>();

            self.AddOptions<AzureServiceBusConfiguration>();
            self.AddSingleton<IConfigureOptions<AzureServiceBusConfiguration>>(sp => new ConfigureNamedOptions<AzureServiceBusConfiguration>(Options.DefaultName,
                opts =>
                {
                    var configuration = sp.GetRequiredService<IConfiguration>();
                    configuration.GetSection("AzureServiceBus").Bind(opts, c => c.ErrorOnUnknownConfiguration = true);
                    configure?.Invoke(opts, sp);

                    if (opts.ConnectionString == null)
                    {
                        var connectiongStringName = opts.ConnectionStringName
                            ?? throw new ArgumentException($"Connection string for {nameof(AzureServiceBusConfiguration)}.{nameof(AzureServiceBusConfiguration.ConnectionStringName)} was not configured");
                        opts.ConnectionString = configuration.GetConnectionString(connectiongStringName)
                            ?? throw new ArgumentException($"Connection string {opts.ConnectionStringName} was not configured");
                    }
                })
            );

            return self;
        }
    }
}