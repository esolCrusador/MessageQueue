using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal static class AzureServiceBusServiceCollectionExtensions
    {
        public static IServiceCollection AddAzureServiceBusMessageQueue(this IServiceCollection self)
        {
            self.TryAddSingleton<AzureServiceBusClientHolder>();
            self.TryAddSingleton<AzureServiceBusNamingConvention>();
            self.TryAddSingleton<IMessageQueue, AzureServiceBusMessageSender>();
            self.TryAddSingleton<IMessageConsumer, AzureServiceBusConsumer>();

            return self;
        }
    }
}