using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace EsoTech.MessageQueue.RabbitMQ
{
    public static class RabbitMqBootstrapper
    {
        public static IServiceCollection AddRabbitMq(this IServiceCollection services, string sectionName)
        {
            return AddRabbitMq(services, (opts, configuration) => configuration.GetSection(sectionName).Bind(opts));
        }

        public static IServiceCollection AddRabbitMq(this IServiceCollection services, Action<RabbitMQConfiguration, IConfiguration> configure)
        {
            services.AddOptions<RabbitMQConfiguration>().Configure(configure);

            RegisterService(services);

            return services;
        }

        private static void RegisterService(IServiceCollection services)
        {
            services.AddSingleton<NamingConvention>();
            services.AddSingleton<RabbitMQClient>();
            services.AddSingleton<MessageSerializer>();
            services.AddSingleton<HashFunction>();

            services.AddSingleton<IMessageConsumer, RabbitMqConsumer>();
            services.AddSingleton<IMessageQueue, RabbitMqMessageQueue>();
        }
    }
}
