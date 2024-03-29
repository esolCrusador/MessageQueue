﻿using EsoTech.MessageQueue.AzureServiceBus;
using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Reflection;
using System.Text.Json;

namespace EsoTech.MessageQueue
{
    public static class MessageQueueBootstrapper
    {
        private static Action<AzureServiceBusConfiguration> update = _ => { };
        public static IServiceCollection AddMessageQueue(this IServiceCollection self,
            string connectionStringName,
            int ackTimeoutMilliseconds = 30000,
            string? clientId = default,
            string? serviceName = default,
            int maxRedeliveryCount = -1,
            int maxConcurrentMessages = 100,
            Action<AzureServiceBusConfiguration>? updateConfiguration = null
        )
        {
            var callingAssembly = Assembly.GetCallingAssembly();
            self.TryAddSingleton<TracerFactory>();
            self.TryAddSingleton<MessageQueueConfigurationFactory>();
            self.TryAddSingleton<MessageSerializer>();
            self.TryAddSingleton(s =>
            {
                var factory = s.GetRequiredService<MessageQueueConfigurationFactory>();
                return factory.Create(callingAssembly, connectionStringName, clientId, ackTimeoutMilliseconds, serviceName, maxRedeliveryCount, maxConcurrentMessages, updateConfiguration ?? update);
            });

            self.AddAzureServiceBusMessageQueue();

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
