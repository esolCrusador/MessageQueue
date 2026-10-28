using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.RabbitMQ;
using EsoTech.MessageQueue.Tests.EventHandlers;
using EsoTech.MessageQueue.Tests.Messages;
using System;
using System.Threading.Tasks;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{
    [Trait("Category", "Slow")]
    [Collection(nameof(RabbitMqCollectionCollection))]
    public sealed class BroadcastFacts : IAsyncLifetime
    {
        private readonly RabbitMqTestFixture _fixture;
        private readonly string _virtualHost = $"/{Guid.NewGuid():n}";
        private readonly System.Collections.Generic.List<ServiceProvider> _serviceProviders = new();

        public BroadcastFacts(RabbitMqTestFixture rabbitMqTestFixture)
        {
            _fixture = rabbitMqTestFixture;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            foreach (var serviceProvider in _serviceProviders)
                await serviceProvider.DisposeAsync();
        }

        private ServiceProvider CreateInstance(string instanceId, Action<IServiceCollection> addHandlers, int maxDeliveryCount = 20)
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
                options.MaxDeliveryCount = maxDeliveryCount;
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
        public async Task Broadcast_Should_Deliver_To_Every_Instance()
        {
            var instance1 = CreateInstance("broadcast-facts-instance-1", services => services.AddEventMessageHandler<BroadcastEventHandler1>());
            var instance2 = CreateInstance("broadcast-facts-instance-2", services => services.AddEventMessageHandler<BroadcastEventHandler1>());
            await Initialize(instance1, instance2);

            var queue = instance1.GetRequiredService<IMessageQueue>();
            await queue.SendEvent(new BroadcastMsg { Text = "hello instances" });

            var handler1 = instance1.GetRequiredService<BroadcastEventHandler1>();
            var handler2 = instance2.GetRequiredService<BroadcastEventHandler1>();
            await WaitUntil(() => handler1.Log.Count == 1 && handler2.Log.Count == 1);

            handler1.Log.Count.Should().Be(1);
            handler2.Log.Count.Should().Be(1);
            handler1.Log[0].Text.Should().Be("hello instances");
            handler2.Log[0].Text.Should().Be("hello instances");
        }

        [Fact]
        public async Task Regular_Event_Should_Remain_Competing()
        {
            var instance1 = CreateInstance("broadcast-facts-instance-1", services => services.AddEventMessageHandler<CompetingEventHandler>());
            var instance2 = CreateInstance("broadcast-facts-instance-2", services => services.AddEventMessageHandler<CompetingEventHandler>());
            await Initialize(instance1, instance2);

            var queue = instance1.GetRequiredService<IMessageQueue>();
            await queue.SendEvent(new BroadcastMsg { Text = "hello workers" });

            var handler1 = instance1.GetRequiredService<CompetingEventHandler>();
            var handler2 = instance2.GetRequiredService<CompetingEventHandler>();
            await WaitUntil(() => handler1.Log.Count + handler2.Log.Count == 1);
            await Task.Delay(TimeSpan.FromSeconds(2));

            (handler1.Log.Count + handler2.Log.Count).Should().Be(1);
        }

        [Fact]
        public async Task Broadcast_Should_Invoke_All_Broadcast_Handlers_In_One_Process()
        {
            var instance = CreateInstance("broadcast-facts-instance-1", services => services
                .AddEventMessageHandler<BroadcastEventHandler1>()
                .AddEventMessageHandler<BroadcastEventHandler2>());
            await Initialize(instance);

            await instance.GetRequiredService<IMessageQueue>().SendEvent(new BroadcastMsg { Text = "hello handlers" });

            var handler1 = instance.GetRequiredService<BroadcastEventHandler1>();
            var handler2 = instance.GetRequiredService<BroadcastEventHandler2>();
            await WaitUntil(() => handler1.Log.Count == 1 && handler2.Log.Count == 1);

            handler1.Log.Count.Should().Be(1);
            handler2.Log.Count.Should().Be(1);
        }

        [Fact]
        public async Task Initialize_Should_Throw_When_Event_Is_Both_Broadcast_And_Regular()
        {
            var instance = CreateInstance("broadcast-facts-instance-1", services => services
                .AddEventMessageHandler<BroadcastEventHandler1>()
                .AddEventMessageHandler<CompetingEventHandler>());

            var invocation = await Assert.ThrowsAsync<InvalidOperationException>(
                () => instance.GetRequiredService<IMessageConsumer>().Initialize(default));

            invocation.Message.Should().Contain(nameof(BroadcastMsg));
        }

        [Fact]
        public async Task Broadcast_Retry_Should_Redeliver_Only_To_Failing_Instance()
        {
            var instance1 = CreateInstance("broadcast-facts-instance-1", services => services.AddEventMessageHandler<FailingBroadcastEventHandler>(), maxDeliveryCount: 3);
            var instance2 = CreateInstance("broadcast-facts-instance-2", services => services.AddEventMessageHandler<BroadcastEventHandler2>());
            await Initialize(instance1, instance2);

            var failingHandler = instance1.GetRequiredService<FailingBroadcastEventHandler>();
            failingHandler.FailTimes = 2;

            await instance1.GetRequiredService<IMessageQueue>().SendEvent(new BroadcastMsg { Text = "retry me" });

            var handler2 = instance2.GetRequiredService<BroadcastEventHandler2>();
            await WaitUntil(() => failingHandler.Log.Count == 1);
            await Task.Delay(TimeSpan.FromSeconds(2));

            failingHandler.Attempts.Should().Be(3);
            handler2.Log.Count.Should().Be(1);
        }
    }
}
