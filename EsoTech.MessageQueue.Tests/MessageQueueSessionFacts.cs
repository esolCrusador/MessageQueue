using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Abstractions.Aggregations;
using EsoTech.MessageQueue.AzureServiceBus;
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
            public Fast() : base(CreateServiceProvider())
            {
            }

            public static ServiceProvider CreateServiceProvider()
            {
                var serviceCollection = ConfigureCommonServices(new ServiceCollection());
                serviceCollection.AddFakeMessageQueue();

                var serviceProvider = serviceCollection.BuildServiceProvider();

                return serviceProvider;
            }
        }

        [Trait("Category", "Slow")]
        public sealed class Slow : MessageQueueSessionFacts
        {
            public Slow() : base(ConfigureCommonServices(new ServiceCollection())
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .SuppressContinuousPolling()
                .AddMessageQueue("TestServiceBusConnectionString")
                .BuildServiceProvider()
            )
            {
            }
        }

        private static IServiceCollection ConfigureCommonServices(IServiceCollection services)
        {
            return services.AddLogging()
                    .TryAddMessageQueueSession()
                    .AddSingleton<FooEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<FooEventHandler>())
                    .AddSingleton<EnvelopedFooEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<EnvelopedFooEventHandler>())
                    .AddSingleton<BarEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<BarEventHandler>())
                    .AddEventMessageHandler<MultiEventHandler1>()
                    .AddEventMessageHandler<MultiEventHandler2>()
                    .AddCommandMessageHandler<BarCommandHandler>()
                    .AddEventMessageHandler<SessionMessageHandler>()
                    .AddMessageAggregator<SessionMsg, List<SessionMsg>>((list, msg) => list.Add(msg));
        }

        private MessageQueueSessionFacts(ServiceProvider serviceProvider)
        {
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
                var _queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                _queue.SendEvent(new FooMsg());
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
                var _queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                _queue.SendEvent(msg);
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
                var _queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                _queue.SendEvents(new List<FooMsg> { msg1, msg2 });
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
                var _queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                _queue.SendEvent(msg1);
                _queue.SendEvent(msg2);
                (await _subscriber.TryHandleNext()).Should().BeFalse();
            }
            await _subscriber.HandleNext();
            await _subscriber.HandleNext();

            _fooHandler.Log.Should().HaveCount(2);
            _fooHandler.Log.Should().ContainEquivalentOf(msg1);
            _fooHandler.Log.Should().ContainEquivalentOf(msg2);
        }

        [Fact]
        public async Task HandleNext_Should_Send_Events_DelayedEvents_Commands()
        {
            var msg1 = new FooMsg { Text = "some text 1" };
            var msg2 = new FooMsg { Text = "some text 2" };

            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var _queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                _queue.SendEvent(msg1);
                _queue.SendEvent(msg2);
                _queue.SendEvent(new FooMsg { Text = "Delayed" }, TimeSpan.FromMilliseconds(100));
                _queue.SendCommand(new BarMsg { Text = "Command" });
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

        [Fact]
        public async Task Should_Aggregate_Events()
        {
            var msg1 = new SessionMsg { Message = "1" };
            var msg2 = new SessionMsg { Message = "2" };

            await using (var scope = _serviceProvier.CreateAsyncScope())
            {
                var _queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                _queue.SendEvent(msg1);
                _queue.SendEvent(msg2);
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
                var _queue = scope.ServiceProvider.GetRequiredService<IMessageQueueSession>();
                _queue.SendEvent(msg1);
                _queue.SendEvent(msg2);
            }

            await _subscriber.HandleNext();
            await _subscriber.HandleNext();
            _sessionHandler.AggregatedLog.Should().HaveCount(0);
            _sessionHandler.Log.Should().HaveCount(1);
            _sessionHandler.Log.Single().Should().BeEquivalentTo(msg1);
        }
    }
}
