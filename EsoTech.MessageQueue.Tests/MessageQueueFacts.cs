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
using EsoTech.MessageQueue.Tests.Handlers;
using System.Threading;

namespace EsoTech.MessageQueue.Tests
{
    public abstract class MessageQueueFacts : IAsyncLifetime
    {
        [Trait("Category", "Fast")]
        public sealed class Fast : MessageQueueFacts
        {
            public Fast() : base(CreateServiceProvider())
            {
            }

            public static IServiceProvider CreateServiceProvider()
            {
                var serviceCollection = new ServiceCollection()
                    .AddLogging()
                    .AddSingleton<FooHandler>().AddSingleton<IMessageHandler>(ctx => ctx.GetRequiredService<FooHandler>())
                    .AddSingleton<EnvelopedFooHandler>().AddSingleton<IMessageHandler>(ctx => ctx.GetRequiredService<EnvelopedFooHandler>())
                    .AddSingleton<BarHandler>().AddSingleton<IMessageHandler>(ctx => ctx.GetRequiredService<BarHandler>())
                    .AddMessageHandler<MultiHandler1>()
                    .AddMessageHandler<MultiHandler2>();

                serviceCollection.AddFakeMessageQueue();

                var serviceProvider = serviceCollection.BuildServiceProvider();

                return serviceProvider;
            }
        }

        [Trait("Category", "Integration")]
        public sealed class Integration : MessageQueueFacts
        {
            public Integration() : base(new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .AddMessageHandler<FooHandler>()
                .AddMessageHandler<EnvelopedFooHandler>()
                .AddMessageHandler<BarHandler>()
                .AddMessageHandler<MultiHandler1>()
                .AddMessageHandler<MultiHandler2>()
                .SuppressContinuousPolling()
                .AddMessageQueue("TestServiceBusConnectionString")
                .BuildServiceProvider()
            )
            {
            }
        }

        protected MessageQueueFacts(IServiceProvider serviceProvider)
        {
            _serviceProvier = serviceProvider;

            _subscriber = serviceProvider.GetRequiredService<IMessageConsumer>();
            _queue = serviceProvider.GetRequiredService<IMessageQueue>();
            _fooHandler = serviceProvider.GetRequiredService<FooHandler>();
            _envelopedFooHandler = serviceProvider.GetRequiredService<EnvelopedFooHandler>();
            _barHandler = serviceProvider.GetRequiredService<BarHandler>();
            _handler1 = serviceProvider.GetRequiredService<MultiHandler1>();
            _handler2 = serviceProvider.GetRequiredService<MultiHandler2>();
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

        private readonly IServiceProvider _serviceProvier;

        private readonly IMessageConsumer _subscriber;
        private readonly IMessageQueue _queue;

        private readonly FooHandler _fooHandler;

        private readonly EnvelopedFooHandler _envelopedFooHandler;

        private readonly BarHandler _barHandler;

        private readonly MultiHandler1 _handler1;

        private readonly MultiHandler2 _handler2;

        [Fact]
        public async Task Send_Should_Not_Invoke_Handlers()
        {
            await _queue.Send(new FooMsg());

            _fooHandler.Log.OfType<object>().Concat(_barHandler.Log).Should().BeEmpty();

            await _subscriber.HandleNext(); // To remove the message from the queue so that the next test can pass.
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler()
        {
            var msg = new FooMsg { Text = "some text" };

            await _queue.Send(msg);
            await _subscriber.HandleNext();

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler_For_Enveloped_Messages()
        {
            var msg = new FooMsg { Text = "some text" };
            var enveloped = new Envelope<FooMsg>
            {
                Payload = msg
            };

            await _queue.Send(enveloped);
            await _subscriber.HandleNext();

            _envelopedFooHandler.Log.Single().Payload.Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Invoke_Appropriate_Handler()
        {
            var msg = new FooMsg { Text = "some text" };

            await _queue.Send(msg);
            while (!await _subscriber.TryHandleNext()) { }

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task HandleNext_Should_Not_Invoke_Inappropriate_Handlers()
        {
            await _queue.Send(new FooMsg());
            await _subscriber.HandleNext();

            _barHandler.Log.Should().BeEmpty();
        }

        [Fact]
        public async Task TryHandleNext_Should_Not_Invoke_Inappropriate_Handlers()
        {
            await _queue.Send(new FooMsg());
            while (!await _subscriber.TryHandleNext()) { }

            _barHandler.Log.Should().BeEmpty();
        }

        [Fact]
        public async Task HandleNext_Should_Process_Message_Received_After_The_Call()
        {
            var msg = new FooMsg { Text = "some text" };

            var handleNextTask = _subscriber.HandleNext();
            await _queue.Send(msg);
            await handleNextTask;

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Not_Process_Message_Received_After_The_Call()
        {
            var msg = new FooMsg { Text = "some text" };

            using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.Zero);
            var tryHandleNextTask = _subscriber.TryHandleNext(cancellationTokenSource.Token);
            await _queue.Send(msg);
            (await tryHandleNextTask).Should().BeFalse();

            await _subscriber.HandleNext(); // To remove the message from the queue so that the next test can pass.
        }

        [Fact]
        public async Task HandleNext_Should_Skip_Invalid_Messages()
        {
            var invalidMsg = new NotHandledMessage { Text = "just a string that has no handlers" };
            var validMsg = new FooMsg { Text = "some text" };

            await _queue.Send(invalidMsg);
            await _queue.Send(validMsg);

            await _subscriber.HandleNext();

            _fooHandler.Log.Single().Text.Should().Be(validMsg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Skip_Invalid_Messages()
        {
            var invalidMsg = new NotHandledMessage { Text = "just a string that has no handlers" };
            var validMsg = new FooMsg { Text = "some text" };

            await _queue.Send(invalidMsg);
            await _queue.Send(validMsg);

            while (!await _subscriber.TryHandleNext()) { }

            _fooHandler.Log.Single().Text.Should().Be(validMsg.Text);
        }

        [Fact]
        public async Task Send_Should_Broadcast_To_More_Than_One_Handler()
        {
            await _queue.Send(new MultiMsg());
            (await _subscriber.TryHandleNext()).Should().BeTrue();
            _handler1.Log.Should().HaveCount(1);
            _handler2.Log.Should().HaveCount(1);
        }
    }
}
