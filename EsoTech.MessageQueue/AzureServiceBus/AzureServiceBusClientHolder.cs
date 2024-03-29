﻿using Azure.Messaging.ServiceBus;
using System;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal class AzureServiceBusClientHolder
    {
        public ServiceBusClient Instance { get; }

        public AzureServiceBusClientHolder(MessageQueueConfiguration configuration)
        {
            try
            {
                Instance = new ServiceBusClient(configuration.AzureServiceBusConfiguration.ConnectionString);
            }
            catch (FormatException ex)
            {
                throw new FormatException($"Could not parse connection string \"{configuration.AzureServiceBusConfiguration.ConnectionString}\"", ex);
            }
        }
    }
}
