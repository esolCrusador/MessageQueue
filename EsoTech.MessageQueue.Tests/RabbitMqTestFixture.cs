using EsoTech.MessageQueue.RabbitMQ;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using System;
using System.Threading.Tasks;
using Testcontainers.RabbitMq;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{
    [CollectionDefinition(nameof(RabbitMqCollectionCollection))]
    public class RabbitMqCollectionCollection : ICollectionFixture<RabbitMqTestFixture>
    {
    }
    public class RabbitMqTestFixture : IAsyncLifetime
    {
        private readonly RabbitMqContainer _container;

        public RabbitMqTestFixture()
        {
            _container = new RabbitMqBuilder()
                .WithImage("rabbitmq:management")
                .WithPortBinding(5672, true)
                .WithPortBinding(15672, true)
                .Build();
        }
        public Task InitializeAsync() => _container.StartAsync();
        public Task DisposeAsync() => _container.StopAsync();

        public void Configure(RabbitMQConfiguration opts, IConfiguration configuration)
        {
            var connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(_container.GetConnectionString())
            };

            opts.Connection.Host = connectionFactory.HostName;
            opts.Connection.User = connectionFactory.UserName;
            opts.Connection.Password = connectionFactory.Password;
            opts.Connection.Port = connectionFactory.Port;
            opts.Connection.ManagementPort = _container.GetMappedPublicPort(15672);
            opts.Connection.VirtualHost = $"/{Guid.NewGuid():n}";
        }
    }
}
