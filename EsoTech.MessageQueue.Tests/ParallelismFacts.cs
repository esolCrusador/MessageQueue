using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using EsoTech.MessageQueue.RabbitMQ;
using EsoTech.MessageQueue.Testing;
using EsoTech.MessageQueue.Tests.EventHandlers;
using EsoTech.MessageQueue.Tests.Messages;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{
    public abstract class ParallelismFacts : IAsyncLifetime
    {
        private readonly ServiceProvider _serviceProvier;
        private readonly IMessageConsumer _subscriber;
        private readonly FooEventHandler _fooHandler;
        private readonly FooEventDelegateHandler _fooDelegateHandler;
        private readonly AzureServiceBusManager? _azureServiceBusManager;
        private readonly IMessageQueue _queue;

        //[Trait("Category", "Slow")]
        //public sealed class SlowAzureServiceBus : RetryFacts
        //{
        //    public SlowAzureServiceBus() : base(new ServiceCollection()
        //        .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
        //        .SuppressContinuousPolling()
        //        .AddMessageQueue(opts => opts.AckTimeout = TimeSpan.FromSeconds(1))
        //        .AddAzureServiceBusMessageQueue(opts => opts.ConnectionStringName = "TestServiceBusConnectionString")
        //    )
        //    {
        //    }
        //}

        private static IServiceCollection ConfigureCommonServices(IServiceCollection services)
        {
            return services.AddLogging()
                    .AddEventMessageHandler<FooEventDelegateHandler>()
                    .AddEventMessageHandler<FooEventHandler>();
        }

        private ParallelismFacts(IServiceCollection services)
        {
            ConfigureCommonServices(services);
            var serviceProvider = services.BuildServiceProvider();
            _serviceProvier = serviceProvider;

            _subscriber = serviceProvider.GetRequiredService<IMessageConsumer>();
            _fooHandler = serviceProvider.GetRequiredService<FooEventHandler>();
            _fooDelegateHandler = serviceProvider.GetRequiredService<FooEventDelegateHandler>();
            _azureServiceBusManager = serviceProvider.GetService<AzureServiceBusManager>();
            _queue = serviceProvider.GetRequiredService<IMessageQueue>();
        }

        //[Trait("Category", "Slow")]
        //public sealed class SlowAzureServiceBus : CommandMessageQueueFacts
        //{
        //    public SlowAzureServiceBus() : base(new ServiceCollection()
        //        .AddLogging()
        //        .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
        //        .SuppressContinuousPolling()
        //        .AddMessageQueue()
        //        .AddAzureServiceBusMessageQueue(cfg => cfg.ConnectionStringName = "TestServiceBusConnectionString")
        //    )
        //    {
        //    }

        //    //[Fact]
        //    //[Trait("Category", "Integration")]
        //    //public async Task PurgeAll_Should_Clean_Up_Queues()
        //    //{
        //    //    await (_azureServiceBusManager ?? throw new Exception("No manager")).PurgeAll();
        //    //}
        //}

        [Trait("Category", "Slow")]
        [Collection(nameof(RabbitMqCollectionCollection))]
        public sealed class SlowRabbit : ParallelismFacts
        {
            public SlowRabbit(RabbitMqTestFixture rabbitMqTestFixture) : base(new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .AddMessageQueue()
                .AddRabbitMq(rabbitMqTestFixture.Configure)
            )
            {
            }
        }



        public async Task InitializeAsync()
        {
            if (_serviceProvier.GetService<FakeMessageQueueInitializer>() == null)
                await _serviceProvier.GetRequiredService<IMessageConsumer>().Initialize(default);
        }

        public async Task DisposeAsync()
        {
            if (_queue is IAsyncDisposable disposableQueue)
                await disposableQueue.DisposeAsync();

            await _subscriber.DisposeAsync();
        }

        [Fact]
        public async Task Should_Handle_By_Only_Failed_Handler()
        {
            var text = Guid.NewGuid().ToString("n");
            var subject = new TaskCompletionSource();

            ConcurrentBag<string> receivedMessages = new();
            _fooDelegateHandler.Handler = async (m, _) =>
            {
                receivedMessages.Add(m.Text!);
                await subject.Task;
            };
            await _queue.SendEvent(new FooMsg { Text = "Message1" });
            await _queue.SendEvent(new FooMsg { Text = "Message2" });


            await MessageQueueTestContext.Wait(() => receivedMessages.Contains("Message1") && receivedMessages.Contains("Message2"));
        }

        [Fact]
        public async Task Should_Send_Messages_In_Parallel()
        {
            await _queue.SendEvents(
                 Enumerable.Repeat(0, 100)
                     .Select(_ => new FooMsg { Text = Guid.NewGuid().ToString("n") })
            );

            ConcurrentBag<string> receivedMessages = new();
            _fooDelegateHandler.Handler = (m, _) =>
            {
                receivedMessages.Add(m.Text!);
                return Task.CompletedTask;
            };

            await MessageQueueTestContext.Wait(() => receivedMessages.Count == 100, TimeSpan.FromSeconds(10));
        }
    }
}
