using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
            self.TryAddSingleton<MessageSerializer>();

            var opts = self.AddOptions<MessageQueueConfiguration>();
            if (configure != null)
                opts.Configure(configure);

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
