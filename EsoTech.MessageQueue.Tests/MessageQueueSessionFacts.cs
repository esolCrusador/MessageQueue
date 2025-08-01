using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Abstractions.Aggregations;
using EsoTech.MessageQueue.AzureServiceBus;
using EsoTech.MessageQueue.RabbitMQ;
using EsoTech.MessageQueue.Testing;
using EsoTech.MessageQueue.Tests.CommandHandlers;
using EsoTech.MessageQueue.Tests.EventHandlers;
using EsoTech.MessageQueue.Tests.Messages;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{
    public abstract class MessageQueueSessionFacts : IAsyncLifetime
    {
        [Trait("Category", "Fast")]
        public sealed class Fast : MessageQueueSessionFacts
        {
            public Fast() : base(CreateServices())
            {
            }

            public static IServiceCollection CreateServices()
            {
                var serviceCollection = new ServiceCollection();
                serviceCollection.AddFakeMessageQueue();

                return serviceCollection;
            }

            [Fact]
            public async Task HandleNext_Should_Send_Events_DelayedEvents_Commands()
            {
                var msg1 = new FooMsg { Text = "some text 1" };
                var msg2 = new FooMsg { Text = "some text 2" };

                await using (var scope = _serviceProvier.CreateAsyncScope())
                {
                    var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                    queue.SendEvent(msg1);
                    queue.SendEvent(msg2);
                    queue.SendEvent(new FooMsg { Text = "Delayed" }, TimeSpan.FromMilliseconds(100));
                    queue.SendCommand(new BarMsg { Text = "Command" });
                    (await _subscriber.TryHandleNext()).Should().BeFalse();
                }
                await _subscriber.HandleNext();
                await _subscriber.HandleNext();
                await _subscriber.HandleNext();
                await _subscriber.HandleNext();

                _fooHandler.Log.Should().HaveCount(3);
                _fooHandler.Log.Should().ContainEquivalentOf(msg1);
                _fooHandler.Log.Should().ContainEquivalentOf(msg2);
                _fooHandler.Log.Should().ContainEquivalentOf(new FooMsg { Text = "Delayed" });
                _barCommandHandler.Log.Should().ContainEquivalentOf(new BarMsg { Text = "Command" });
            }
        }

        //[Trait("Category", "Slow")]
        //public sealed class SlowAzureServiceBus : MessageQueueSessionFacts
        //{
        //    public SlowAzureServiceBus() : base(new ServiceCollection()
        //        .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
        //        .AddMessageQueue()
        //        .AddAzureServiceBusMessageQueue(opts => opts.ConnectionStringName = "TestServiceBusConnectionString")
        //    )
        //    {
        //    }

        //    [Fact]
        //    public async Task HandleNext_Should_Send_Events_DelayedEvents_Commands()
        //    {
        //        var msg1 = new FooMsg { Text = "some text 1" };
        //        var msg2 = new FooMsg { Text = "some text 2" };

        //        await using (var scope = _serviceProvier.CreateAsyncScope())
        //        {
        //            var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
        //            queue.SendEvent(msg1);
        //            queue.SendEvent(msg2);
        //            queue.SendEvent(new FooMsg { Text = "Delayed" }, TimeSpan.FromMilliseconds(100));
        //            queue.SendCommand(new BarMsg { Text = "Command" });
        //            (await _subscriber.TryHandleNext()).Should().BeFalse();
        //        }
        //        await _subscriber.HandleNext();
        //        await _subscriber.HandleNext();
        //        await _subscriber.HandleNext();
        //        await _subscriber.HandleNext();

        //        _fooHandler.Log.Should().HaveCount(3);
        //        _fooHandler.Log.Should().ContainEquivalentOf(msg1);
        //        _fooHandler.Log.Should().ContainEquivalentOf(msg2);
        //        _fooHandler.Log.Should().ContainEquivalentOf(new FooMsg { Text = "Delayed" });
        //        _barCommandHandler.Log.Should().ContainEquivalentOf(new BarMsg { Text = "Command" });
        //    }
        //}

        [Trait("Category", "Slow")]
        [Collection(nameof(RabbitMqCollectionCollection))]
        public sealed class SlowRabbit : MessageQueueSessionFacts
        {
            public SlowRabbit(RabbitMqTestFixture rabbitMqTestFixture) : base(new ServiceCollection()
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .AddMessageQueue()
                .AddRabbitMq(rabbitMqTestFixture.Configure)
            )
            {
            }
        }

        private static IServiceCollection ConfigureCommonServices(IServiceCollection services)
        {
            return services.AddLogging()
                    .AddMessageQueueSession()
                    .AddSingleton<FooEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<FooEventHandler>())
                    .AddSingleton<EnvelopedFooEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<EnvelopedFooEventHandler>())
                    .AddSingleton<BarEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<BarEventHandler>())
                    .AddEventMessageHandler<MultiEventHandler1>()
                    .AddEventMessageHandler<MultiEventHandler2>()
                    .AddCommandMessageHandler<BarCommandHandler>()
                    .AddEventMessageHandler<SessionMessageHandler>()
                    .AddMessageAggregator<SessionMsg, List<SessionMsg>>((list, msg) => list.Add(msg));
        }

        private MessageQueueSessionFacts(IServiceCollection services)
        {
            ConfigureCommonServices(services);
            services.SuppressContinuousPolling();
            var serviceProvider = services.BuildServiceProvider();
            _serviceProvier = serviceProvider;

            _subscriber = serviceProvider.GetRequiredService<IMessageConsumer>();
            _fooHandler = serviceProvider.GetRequiredService<FooEventHandler>();
            _sessionHandler = serviceProvider.GetRequiredService<SessionMessageHandler>();
            _barCommandHandler = serviceProvider.GetRequiredService<BarCommandHandler>();
            _envelopedFooHandler = serviceProvider.GetRequiredService<EnvelopedFooEventHandler>();
            _barHandler = serviceProvider.GetRequiredService<BarEventHandler>();
            _handler1 = serviceProvider.GetRequiredService<MultiEventHandler1>();
            _handler2 = serviceProvider.GetRequiredService<MultiEventHandler2>();
            _azureServiceBusManager = serviceProvider.GetService<AzureServiceBusManager>();
        }


        public async Task InitializeAsync()
        {
            if (_serviceProvier.GetService<FakeMessageQueueInitializer>() == null)
                await _serviceProvier.GetRequiredService<IMessageConsumer>().Initialize(default);
        }

        public async Task DisposeAsync()
        {
            await _serviceProvier.DisposeAsync();
        }

        private readonly ServiceProvider _serviceProvier;

        private readonly IMessageConsumer _subscriber;

        private readonly FooEventHandler _fooHandler;
        private readonly SessionMessageHandler _sessionHandler;
        private readonly BarCommandHandler _barCommandHandler;
        private readonly EnvelopedFooEventHandler _envelopedFooHandler;

        private readonly BarEventHandler _barHandler;

        private readonly MultiEventHandler1 _handler1;

        private readonly MultiEventHandler2 _handler2;
        private readonly AzureServiceBusManager? _azureServiceBusManager;

        [Fact]
        public async Task Send_Should_Not_Invoke_Handlers()
        {
            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                queue.SendEvent(new FooMsg());
            }

            _fooHandler.Log.OfType<object>().Concat(_barHandler.Log).Should().BeEmpty();

            await _subscriber.HandleNext(); // To remove the message from the queue so that the next test can pass.
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler()
        {
            var msg = new FooMsg { Text = "some text" };

            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                queue.SendEvent(msg);
                (await _subscriber.TryHandleNext()).Should().BeFalse();
            }
            await _subscriber.HandleNext();

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler_For_Multiple()
        {
            var msg1 = new FooMsg { Text = "some text 1" };
            var msg2 = new FooMsg { Text = "some text 2" };

            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                queue.SendEvents(new List<FooMsg> { msg1, msg2 });
                (await _subscriber.TryHandleNext()).Should().BeFalse();
            }
            await _subscriber.HandleNext();
            await _subscriber.HandleNext();

            _fooHandler.Log.Should().HaveCount(2);
            _fooHandler.Log.Should().ContainEquivalentOf(msg1);
            _fooHandler.Log.Should().ContainEquivalentOf(msg2);
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler_For_Multiple_In_Scope()
        {
            var msg1 = new FooMsg { Text = "some text 1" };
            var msg2 = new FooMsg { Text = "some text 2" };

            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                queue.SendEvent(msg1);
                queue.SendEvent(msg2);
                (await _subscriber.TryHandleNext()).Should().BeFalse();
            }
            await _subscriber.HandleNext();
            await _subscriber.HandleNext();

            _fooHandler.Log.Should().HaveCount(2);
            _fooHandler.Log.Should().ContainEquivalentOf(msg1);
            _fooHandler.Log.Should().ContainEquivalentOf(msg2);
        }

        [Fact]
        public async Task Should_Aggregate_Events()
        {
            var msg1 = new SessionMsg { Message = "1" };
            var msg2 = new SessionMsg { Message = "2" };

            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                queue.SendEvent(msg1);
                queue.SendEvent(msg2);
            }

            await _subscriber.HandleNext();
            _sessionHandler.AggregatedLog.Should().HaveCount(1);
            _sessionHandler.AggregatedLog.Single().Should().BeEquivalentTo(new[] { msg1, msg2 });
        }

        [Fact]
        public async Task Should_Not_Aggregate_Single_Event()
        {
            var msg1 = new SessionMsg { Message = "1" };
            var msg2 = new FooMsg { Text = "2" };

            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                queue.SendEvent(msg1);
                queue.SendEvent(msg2);
            }

            await _subscriber.HandleNext();
            await _subscriber.HandleNext();
            _sessionHandler.AggregatedLog.Should().HaveCount(0);
            _sessionHandler.Log.Should().HaveCount(1);
            _sessionHandler.Log.Single().Should().BeEquivalentTo(msg1);
        }
    }
}
