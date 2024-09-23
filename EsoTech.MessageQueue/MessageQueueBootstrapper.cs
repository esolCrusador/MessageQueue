using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using System;
using System.Text.Json;

namespace EsoTech.MessageQueue
{
    public static class MessageQueueBootstrapper
    {
        public static IServiceCollection AddMessageQueue(this IServiceCollection self, Action<MessageQueueConfiguration> configure) =>
            AddMessageQueue(self, (options, _) => configure(options));
        public static IServiceCollection AddMessageQueue(this IServiceCollection self, Action<MessageQueueConfiguration, IServiceProvider>? configure = null)
        {
            self.TryAddSingleton<TracerFactory>();
            self.TryAddSingleton<MessageQueueConfigurator>();
            self.TryAddSingleton<MessageSerializer>();

            self.AddOptions<MessageQueueConfiguration>();
            self.AddSingleton<IConfigureOptions<MessageQueueConfiguration>>(sp => new ConfigureNamedOptions<MessageQueueConfiguration>(Options.DefaultName,
                opts =>
                {
                    sp.GetRequiredService<IConfiguration>().GetSection("MessageQueue").Bind(opts, c => c.ErrorOnUnknownConfiguration = true);
                    configure?.Invoke(opts, sp);
                    sp.GetRequiredService<MessageQueueConfigurator>().SetDefaults(opts);
                })
            );

            self.AddHostedService<MessageQueueStarter>();

            return self;
        }

        public static JsonSerializerOptions SetupMessageQueueJsonOptions(this JsonSerializerOptions options)
        {
            options.Converters.Add(new MessageConverter());
            return options;
        }
    }
}
