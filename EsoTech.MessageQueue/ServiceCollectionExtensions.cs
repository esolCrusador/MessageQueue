using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using EsoTech.MessageQueue.AzureServiceBus;

namespace EsoTech.MessageQueue
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddMessageQueue(this IServiceCollection self, 
            string connectionStringName,
            int ackTimeoutMilliseconds = 30000,
            string clientId = default,
            string serviceName = default,
            int maxRedeliveryCount = -1,
            int maxConcurrentMessages = 100)
        {
            self.TryAddSingleton<TracerFactory>();
            self.TryAddSingleton<MessageQueueConfigurationFactory>();
            self.TryAddSingleton<MessageSerializer>();
            self.TryAddSingleton(s =>
            {
                var factory = s.GetRequiredService<MessageQueueConfigurationFactory>();
                return factory.Create(connectionStringName, clientId, ackTimeoutMilliseconds, serviceName, maxRedeliveryCount, maxConcurrentMessages);
            });

            self.AddAzureServiceBusMessageQueue();

            self.AddHostedService<MessageQueueStarter>();

            return self;
        }
    }
}
