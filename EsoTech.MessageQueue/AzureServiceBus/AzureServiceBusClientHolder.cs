using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;
using System;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal class AzureServiceBusClientHolder
    {
        public ServiceBusClient Instance { get; }

        public AzureServiceBusClientHolder(IOptions<AzureServiceBusConfiguration> messageQueueOptions)
        {
            try
            {
                Instance = new ServiceBusClient(messageQueueOptions.Value.ConnectionString);
            }
            catch (FormatException ex)
            {
                throw new FormatException($"Could not parse connection string \"{messageQueueOptions.Value.ConnectionString}\"", ex);
            }
        }
    }
}
