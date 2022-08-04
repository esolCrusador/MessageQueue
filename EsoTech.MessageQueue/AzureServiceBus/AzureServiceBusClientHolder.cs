using Azure.Messaging.ServiceBus;
using System;
using System.Collections.Generic;
using System.Text;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal class AzureServiceBusClientHolder
    {
        public ServiceBusClient Instance { get; }

        public AzureServiceBusClientHolder(MessageQueueConfiguration configuration) =>
            Instance = new ServiceBusClient(configuration.ConnectionString);
    }
}
