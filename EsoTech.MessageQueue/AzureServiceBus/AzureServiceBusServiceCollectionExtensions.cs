using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal static class AzureServiceBusServiceCollectionExtensions
    {
        public static IServiceCollection AddAzureServiceBusMessageQueue(this IServiceCollection self)
        {
            self.TryAddSingleton<AzureServiceBusClientHolder>();
            self.TryAddSingleton<AzureServiceBusNamingConvention>();
            self.TryAddSingleton<HashFunction>();
            self.TryAddSingleton<IMessageQueue, AzureServiceBusMessageSender>();
            self.TryAddSingleton<IMessageConsumer, AzureServiceBusConsumer>();
            self.TryAddSingleton(s => new AzureServiceBusManager(
                s.GetRequiredService<MessageQueueConfiguration>().AzureServiceBusConfiguration,
                s.GetRequiredService<AzureServiceBusNamingConvention>(),
                s.GetRequiredService<ILogger<AzureServiceBusManager>>())
            );

            return self;
        }
    }
}