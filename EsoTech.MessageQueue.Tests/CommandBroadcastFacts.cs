using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.RabbitMQ;
using EsoTech.MessageQueue.Tests.CommandHandlers;
using EsoTech.MessageQueue.Tests.Messages;
using System;
using System.Threading.Tasks;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{
    [Trait("Category", "Slow")]
    [Collection(nameof(RabbitMqCollectionCollection))]
    public sealed class CommandBroadcastFacts : IAsyncLifetime
    {
        private readonly RabbitMqTestFixture _fixture;
        private readonly string _virtualHost = $"/{Guid.NewGuid():n}";
        private readonly System.Collections.Generic.List<ServiceProvider> _serviceProviders = new();

        public CommandBroadcastFacts(RabbitMqTestFixture rabbitMqTestFixture)
        {
            _fixture = rabbitMqTestFixture;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (var serviceProvider in _serviceProviders)
                await serviceProvider.DisposeAsync();
        }

        private ServiceProvider CreateInstance(string instanceId, Action<IServiceCollection> addHandlers)
        {
            var services = new ServiceCollection()
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .AddLogging()
                .AddMessageQueue(options => options.AckTimeout = TimeSpan.FromSeconds(1));
            addHandlers(services);
            services.AddRabbitMq((options, configuration) =>
            {
                _fixture.Configure(options, configuration);
                options.Connection.VirtualHost = _virtualHost;
                options.InstanceId = instanceId;
            });

            var serviceProvider = services.BuildServiceProvider();
            _serviceProviders.Add(serviceProvider);

            return serviceProvider;
        }

        private static async Task Initialize(params ServiceProvider[] serviceProviders)
        {
            foreach (var serviceProvider in serviceProviders)
                await serviceProvider.GetRequiredService<IMessageConsumer>().Initialize(default);
        }

        private static async Task WaitUntil(Func<bool> condition, TimeSpan? timeout = default)
        {
            var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                    return;

                await Task.Delay(100);
            }
        }

        [Fact]
        public async Task Broadcast_Command_Should_Deliver_To_Every_Instance()
        {
            var instance1 = CreateInstance("command-broadcast-facts-instance-1", services => services.AddCommandMessageHandler<BroadcastCommandHandler1>());
            var instance2 = CreateInstance("command-broadcast-facts-instance-2", services => services.AddCommandMessageHandler<BroadcastCommandHandler1>());
            await Initialize(instance1, instance2);

            var queue = instance1.GetRequiredService<IMessageQueue>();
            await queue.SendCommand(new BroadcastCommandMsg { Text = "hello instances" });

            var handler1 = instance1.GetRequiredService<BroadcastCommandHandler1>();
            var handler2 = instance2.GetRequiredService<BroadcastCommandHandler1>();
            await WaitUntil(() => handler1.Log.Count == 1 && handler2.Log.Count == 1);

            handler1.Log.Count.Should().Be(1);
            handler2.Log.Count.Should().Be(1);
            handler1.Log[0].Text.Should().Be("hello instances");
            handler2.Log[0].Text.Should().Be("hello instances");
        }

        [Fact]
        public async Task Regular_Command_Should_Remain_Competing()
        {
            var instance1 = CreateInstance("command-broadcast-facts-instance-1", services => services.AddCommandMessageHandler<WorkCommandHandler>());
            var instance2 = CreateInstance("command-broadcast-facts-instance-2", services => services.AddCommandMessageHandler<WorkCommandHandler>());
            await Initialize(instance1, instance2);

            var queue = instance1.GetRequiredService<IMessageQueue>();
            await queue.SendCommand(new WorkCommandMsg { Text = "hello workers" });

            var handler1 = instance1.GetRequiredService<WorkCommandHandler>();
            var handler2 = instance2.GetRequiredService<WorkCommandHandler>();
            await WaitUntil(() => handler1.Log.Count + handler2.Log.Count == 1);
            await Task.Delay(TimeSpan.FromSeconds(2));

            (handler1.Log.Count + handler2.Log.Count).Should().Be(1);
        }

        [Fact]
        public async Task Initialize_Should_Throw_When_Command_Is_Both_Broadcast_And_Regular()
        {
            var instance = CreateInstance("command-broadcast-facts-instance-1", services => services
                .AddCommandMessageHandler<BroadcastCommandHandler1>()
                .AddCommandMessageHandler<CompetingCommandHandler>());

            var invocation = await Assert.ThrowsAsync<InvalidOperationException>(
                () => instance.GetRequiredService<IMessageConsumer>().Initialize(default));

            invocation.Message.Should().Contain(nameof(BroadcastCommandMsg));
        }
    }
}
