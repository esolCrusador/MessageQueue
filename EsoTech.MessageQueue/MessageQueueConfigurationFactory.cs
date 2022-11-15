using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using System;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using System.Linq;
using Microsoft.Extensions.Configuration;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;

namespace EsoTech.MessageQueue
{
    internal class MessageQueueConfigurationFactory
    {
        private readonly IConfiguration _configuration;
        private readonly IHttpContextAccessor? _httpContextAccessor;
        private readonly ContinuousPollingSuppressor? _continuousPollingSuppressor;

        public MessageQueueConfigurationFactory(IConfiguration configuration, IHttpContextAccessor? httpContextAccessor, ContinuousPollingSuppressor? continuousPollingSuppressor)
        {
            _configuration = configuration;
            _httpContextAccessor = httpContextAccessor;
            _continuousPollingSuppressor = continuousPollingSuppressor;
        }

        public MessageQueueConfigurationFactory(IConfiguration configuration, IHttpContextAccessor httpContextAccessor)
            : this(configuration, httpContextAccessor, null)
        {
        }

        public MessageQueueConfigurationFactory(IConfiguration configuration, ContinuousPollingSuppressor continuousPollingSuppressor)
    :       this(configuration, null, continuousPollingSuppressor)
        {
        }

        public MessageQueueConfigurationFactory(IConfiguration configuration)
            : this(configuration, null, null)
        {
        }

        public MessageQueueConfiguration Create(Assembly callingAssembly, string connectionStringName, string? clientId, int ackTimeoutMilliseconds, string? serviceName, int maxRedeliveryCount, int maxConcurrentMessages, Action<AzureServiceBusConfiguration> updateConfiguration)
        {
            if (serviceName == null)
                serviceName = _httpContextAccessor?.HttpContext?.RequestServices?.GetService<IHostEnvironment>()?.ApplicationName ??
                              callingAssembly?.GetName().Name?.Split('.').Skip(1).First()
                              ?? throw new ArgumentException("Could not identify service name");

            var serviceBusConfiguration = new AzureServiceBusConfiguration(_configuration.GetConnectionString(connectionStringName) ?? connectionStringName);
            updateConfiguration(serviceBusConfiguration);

            return new MessageQueueConfiguration(
                (clientId ?? _configuration.GetValue<string>("WEBSITE_SITE_NAME", serviceName)).ToLowerInvariant(),
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
