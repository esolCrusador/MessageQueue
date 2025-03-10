using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using EsoTech.MessageQueue.Testing;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.Configuration;
using EsoTech.MessageQueue.Tests.Messages;
using System.Threading;
using EsoTech.MessageQueue.Tests.EventHandlers;
using EsoTech.MessageQueue.AzureServiceBus;
using System.Collections.Generic;

namespace EsoTech.MessageQueue.Tests
{
    public abstract class EventMessageQueueFacts : IAsyncLifetime
    {
        [Trait("Category", "Fast")]
        public sealed class Fast : EventMessageQueueFacts
        {
            public Fast() : base(CreateServiceProvider())
            {
            }

            public static IServiceProvider CreateServiceProvider()
            {
                var serviceCollection = new ServiceCollection()
                    .AddLogging()
                    .AddSingleton<FooEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<FooEventHandler>())
                    .AddSingleton<EnvelopedFooEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<EnvelopedFooEventHandler>())
                    .AddSingleton<BarEventHandler>().AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<BarEventHandler>())
                    .AddEventMessageHandler<MultiEventHandler1>()
                    .AddEventMessageHandler<MultiEventHandler2>();

                serviceCollection.AddFakeMessageQueue();

                var serviceProvider = serviceCollection.BuildServiceProvider();

                return serviceProvider;
            }
        }

        [Trait("Category", "Slow")]
        public sealed class Slow : EventMessageQueueFacts
        {
            public Slow() : base(new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .AddEventMessageHandler<FooEventHandler>()
                .AddEventMessageHandler<EnvelopedFooEventHandler>()
                .AddEventMessageHandler<BarEventHandler>()
                .AddEventMessageHandler<MultiEventHandler1>()
                .AddEventMessageHandler<MultiEventHandler2>()
                .AddEventMessageHandler<GenericLongRuningEventDelegateHandler<LongRunningMessage>>()
                .SuppressContinuousPolling()
                .AddMessageQueue()
                .AddAzureServiceBusMessageQueue(opts => opts.ConnectionStringName = "TestServiceBusConnectionString")
                .BuildServiceProvider()
            )
            {
            }

            //[Fact]
            //[Trait("Category", "Integration")]
            //public async Task PurgeAll_Should_Clean_Up_Topics()
            //{
            //    await (_azureServiceBusManager ?? throw new Exception("No manager")).PurgeAll();
            //}

            //[Fact]
            //public async Task Should_Continue_Handling_For_Long_Running_Task()
            //{
            //    var _fooLongRuningEventDelegateHandler = _serviceProvider.GetRequiredService<GenericLongRuningEventDelegateHandler<LongRunningMessage>>();
            //    _fooLongRuningEventDelegateHandler.Handler = async (msg, cancellation) =>
            //        await Task.Delay(TimeSpan.FromSeconds(45), cancellation);

            //    await _queue.SendEvent(new LongRunningMessage());

            //    using var cs = new CancellationTokenSource(TimeSpan.FromSeconds(50));
            //    (await _subscriber.TryHandleNext(cs.Token)).Should().BeTrue();
            //}
        }

        private EventMessageQueueFacts(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            _subscriber = serviceProvider.GetRequiredService<IMessageConsumer>();
            _queue = serviceProvider.GetRequiredService<IMessageQueue>();
            _fooHandler = serviceProvider.GetRequiredService<FooEventHandler>();
            _envelopedFooHandler = serviceProvider.GetRequiredService<EnvelopedFooEventHandler>();
            _barHandler = serviceProvider.GetRequiredService<BarEventHandler>();
            _handler1 = serviceProvider.GetRequiredService<MultiEventHandler1>();
            _handler2 = serviceProvider.GetRequiredService<MultiEventHandler2>();
            _azureServiceBusManager = serviceProvider.GetService<AzureServiceBusManager>();
        }


        public async Task InitializeAsync()
        {
            if (_serviceProvider.GetService<FakeMessageQueueInitializer>() == null)
                await _serviceProvider.GetRequiredService<IMessageConsumer>().Initialize(default);
        }

        public async Task DisposeAsync()
        {
            if (_queue is IAsyncDisposable disposableQueue)
                await disposableQueue.DisposeAsync();

            await _subscriber.DisposeAsync();
        }

        protected readonly IServiceProvider _serviceProvider;

        private readonly IMessageConsumer _subscriber;
        private readonly IMessageQueue _queue;

        private readonly FooEventHandler _fooHandler;

        private readonly EnvelopedFooEventHandler _envelopedFooHandler;

        private readonly BarEventHandler _barHandler;

        private readonly MultiEventHandler1 _handler1;

        private readonly MultiEventHandler2 _handler2;
        private readonly AzureServiceBusManager? _azureServiceBusManager;
        private class LongRunningMessage
        {
        }

        [Fact]
        public async Task Send_Should_Not_Invoke_Handlers()
        {
            await _queue.SendEvent(new FooMsg());

            _fooHandler.Log.OfType<object>().Concat(_barHandler.Log).Should().BeEmpty();

            await _subscriber.HandleNext(); // To remove the message from the queue so that the next test can pass.
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler()
        {
            var msg = new FooMsg { Text = "some text" };

            await _queue.SendEvent(msg);
            await _subscriber.HandleNext();

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler_For_Multiple()
        {
            var msg1 = new FooMsg { Text = "some text 1" };
            var msg2 = new FooMsg { Text = "some text 2" };

            await _queue.SendEvents(new List<FooMsg> { msg1, msg2 });
            await _subscriber.HandleNext();
            await _subscriber.HandleNext();

            _fooHandler.Log.Should().HaveCount(2);
            _fooHandler.Log.Should().ContainEquivalentOf(msg1);
            _fooHandler.Log.Should().ContainEquivalentOf(msg2);
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler_For_Enveloped_Messages()
        {
            var msg = new FooMsg { Text = "some text" };
            var enveloped = new Envelope<FooMsg>
            {
                Payload = msg
            };

            await _queue.SendEvent(enveloped);
            await _subscriber.HandleNext();

            string payloadText = _envelopedFooHandler.Log.Single().Payload?.Text ?? throw new InvalidOperationException();
            payloadText.Should().Be(msg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Invoke_Appropriate_Handler()
        {
            var msg = new FooMsg { Text = "some text" };

            await _queue.SendEvent(msg);
            while (!await _subscriber.TryHandleNext()) { }

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Invoke_Appropriate_Handler_With_Delay()
        {
            var msg = new FooDelayedMessage { Text = "some text" };

            await _queue.SendEvent(msg, TimeSpan.FromSeconds(1));
            await Task.Delay(TimeSpan.FromSeconds(2));
            (await _subscriber.TryHandleNext()).Should().BeTrue();

            _fooHandler.DelayedLog.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task HandleNext_Should_Not_Invoke_Inappropriate_Handlers()
        {
            await _queue.SendEvent(new FooMsg());
            await _subscriber.HandleNext();

            _barHandler.Log.Should().BeEmpty();
        }

        [Fact]
        public async Task TryHandleNext_Should_Not_Invoke_Inappropriate_Handlers()
        {
            await _queue.SendEvent(new FooMsg());
            while (!await _subscriber.TryHandleNext()) { }

            _barHandler.Log.Should().BeEmpty();
        }

        [Fact]
        public async Task HandleNext_Should_Process_Message_Received_After_The_Call()
        {
            var msg = new FooMsg { Text = "some text" };

            var handleNextTask = _subscriber.HandleNext();
            await _queue.SendEvent(msg);
            await handleNextTask;

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Not_Process_Message_Received_After_The_Call()
        {
            var msg = new FooMsg { Text = "some text" };

            using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.Zero);
            var tryHandleNextTask = _subscriber.TryHandleNext(cancellationTokenSource.Token);
            await _queue.SendEvent(msg);
            (await tryHandleNextTask).Should().BeFalse();

            await _subscriber.HandleNext(); // To remove the message from the queue so that the next test can pass.
        }

        [Fact]
        public async Task HandleNext_Should_Skip_Invalid_Messages()
        {
            var invalidMsg = new NotHandledMessage { Text = "just a string that has no handlers" };
            var validMsg = new FooMsg { Text = "some text" };

            await _queue.SendEvent(invalidMsg);
            await _queue.SendEvent(validMsg);

            await _subscriber.HandleNext();

            _fooHandler.Log.Single().Text.Should().Be(validMsg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Skip_Invalid_Messages()
        {
            var invalidMsg = new NotHandledMessage { Text = "just a string that has no handlers" };
            var validMsg = new FooMsg { Text = "some text" };

            await _queue.SendEvent(invalidMsg);
            await _queue.SendEvent(validMsg);

            while (!await _subscriber.TryHandleNext()) { }

            _fooHandler.Log.Single().Text.Should().Be(validMsg.Text);
        }

        [Fact]
        public async Task Send_Should_Broadcast_To_More_Than_One_Handler()
        {
            await _queue.SendEvent(new MultiMsg());
            (await _subscriber.TryHandleNext()).Should().BeTrue();
            _handler1.Log.Should().HaveCount(1);
            _handler2.Log.Should().HaveCount(1);
        }
    }
}
