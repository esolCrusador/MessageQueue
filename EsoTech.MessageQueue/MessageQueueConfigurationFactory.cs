using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Linq;
using System.Reflection;

namespace EsoTech.MessageQueue
{
    internal class MessageQueueConfigurationFactory
    {
        private readonly IConfiguration _configuration;
        private readonly ContinuousPollingSuppressor? _continuousPollingSuppressor;

        public MessageQueueConfigurationFactory(IConfiguration configuration, ContinuousPollingSuppressor? continuousPollingSuppressor)
        {
            _configuration = configuration;
            _continuousPollingSuppressor = continuousPollingSuppressor;
        }

        public MessageQueueConfigurationFactory(IConfiguration configuration)
            : this(configuration, null)
        {
        }

        public MessageQueueConfiguration Create(Assembly callingAssembly, string connectionStringName, string? clientId, int ackTimeoutMilliseconds, string? serviceName, int maxRedeliveryCount, int maxConcurrentMessages, Action<AzureServiceBusConfiguration> updateConfiguration)
        {
            if (serviceName == null)
                serviceName = callingAssembly?.GetName().Name?.Split('.').Skip(1).First()
                              ?? throw new ArgumentException("Could not identify service name");

            var serviceBusConfiguration = new AzureServiceBusConfiguration(_configuration.GetConnectionString(connectionStringName) ?? connectionStringName);
            updateConfiguration(serviceBusConfiguration);

            return new MessageQueueConfiguration(
                (clientId ?? _configuration.GetValue<string>("WEBSITE_SITE_NAME", serviceName)
                    ?? throw new ArgumentException($"Please pass {nameof(clientId)} of configure WEBSITE_SITE_NAME")
                ).ToLowerInvariant(),
                serviceName,
                ackTimeoutMilliseconds,
                _continuousPollingSuppressor == null,
                maxRedeliveryCount,
                maxConcurrentMessages,
                serviceBusConfiguration
            );
        }
    }
}
