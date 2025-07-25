﻿using Azure.Messaging.ServiceBus;
using EsoTech.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.Options;
using System;

namespace EsoTech.MessageQueue.AzureServicebus
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
