using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.Configuration;
using System;
using System.Linq;
using System.Reflection;

namespace EsoTech.MessageQueue
{
    internal class MessageQueueConfigurator
    {
        private readonly IConfiguration _configuration;

        public MessageQueueConfigurator(IConfiguration configuration) => _configuration = configuration;

        public void SetDefaults(MessageQueueConfiguration options)
        {
            options.ServiceName ??= Assembly.GetCallingAssembly()?.GetName().Name?.Split('.').Skip(1).First()
                    ?? throw new ArgumentException("Could not identify service name");

            options.ClientId ??= _configuration.GetValue("WEBSITE_SITE_NAME", options.ServiceName!)!.ToLowerInvariant();
        }
    }
}
