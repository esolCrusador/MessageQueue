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
using EsoTech.MessageQueue.Tests.CommandHandlers;

namespace EsoTech.MessageQueue.Tests
{
    public abstract class CommandMessageQueueFacts : IAsyncLifetime
    {
        [Trait("Category", "Fast")]
        public sealed class Fast : CommandMessageQueueFacts
        {
            public Fast() : base(CreateServiceProvider())
            {
            }

            public static IServiceProvider CreateServiceProvider()
            {
                var serviceCollection = new ServiceCollection()
                    .AddLogging()
                    .AddSingleton<FooCommandHandler>().AddSingleton<ICommandMessageHandler>(ctx => ctx.GetRequiredService<FooCommandHandler>())
                    .AddSingleton<EnvelopedFooCommandHandler>().AddSingleton<ICommandMessageHandler>(ctx => ctx.GetRequiredService<EnvelopedFooCommandHandler>())
                    .AddSingleton<BarCommandHandler>().AddSingleton<ICommandMessageHandler>(ctx => ctx.GetRequiredService<BarCommandHandler>())
                    .AddCommandMessageHandler<MultiCommandHandler1>()
                    .AddCommandMessageHandler<MultiCommandHandler2>();

                serviceCollection.AddFakeMessageQueue();

                var serviceProvider = serviceCollection.BuildServiceProvider();

                return serviceProvider;
            }
        }

        [Trait("Category", "Integration")]
        public sealed class Integration : CommandMessageQueueFacts
        {
            public Integration() : base(new ServiceCollection()
                .AddLogging()
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .AddCommandMessageHandler<FooCommandHandler>()
                .AddCommandMessageHandler<EnvelopedFooCommandHandler>()
                .AddCommandMessageHandler<BarCommandHandler>()
                .AddCommandMessageHandler<MultiCommandHandler1>()
                .AddCommandMessageHandler<MultiCommandHandler2>()
                .SuppressContinuousPolling()
                .AddMessageQueue("TestServiceBusConnectionString")
                .BuildServiceProvider()
            )
            {
            }
        }

        protected CommandMessageQueueFacts(IServiceProvider serviceProvider)
        {
            _serviceProvier = serviceProvider;

            _subscriber = serviceProvider.GetRequiredService<IMessageConsumer>();
            _queue = serviceProvider.GetRequiredService<IMessageQueue>();
            _fooHandler = serviceProvider.GetRequiredService<FooCommandHandler>();
            _envelopedFooHandler = serviceProvider.GetRequiredService<EnvelopedFooCommandHandler>();
            _barHandler = serviceProvider.GetRequiredService<BarCommandHandler>();
            _handler1 = serviceProvider.GetRequiredService<MultiCommandHandler1>();
            _handler2 = serviceProvider.GetRequiredService<MultiCommandHandler2>();
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

        private readonly FooCommandHandler _fooHandler;

        private readonly EnvelopedFooCommandHandler _envelopedFooHandler;

        private readonly BarCommandHandler _barHandler;

        private readonly MultiCommandHandler1 _handler1;

        private readonly MultiCommandHandler2 _handler2;

        [Fact]
        public async Task Send_Should_Not_Invoke_Handlers()
        {
            await _queue.SendCommand(new FooMsg());

            _fooHandler.Log.OfType<object>().Concat(_barHandler.Log).Should().BeEmpty();

            await _subscriber.HandleNext(); // To remove the message from the queue so that the next test can pass.
        }

        [Fact]
        public async Task HandleNext_Should_Invoke_Appropriate_Handler()
        {
            var msg = new FooMsg { Text = "some text" };

            await _queue.SendCommand(msg);
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

            await _queue.SendCommand(enveloped);
            await _subscriber.HandleNext();

            _envelopedFooHandler.Log.Single().Payload.Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Invoke_Appropriate_Handler()
        {
            var msg = new FooMsg { Text = "some text" };

            await _queue.SendCommand(msg);
            while (!await _subscriber.TryHandleNext()) { }

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task HandleNext_Should_Not_Invoke_Inappropriate_Handlers()
        {
            await _queue.SendCommand(new FooMsg());
            await _subscriber.HandleNext();

            _barHandler.Log.Should().BeEmpty();
        }

        [Fact]
        public async Task TryHandleNext_Should_Not_Invoke_Inappropriate_Handlers()
        {
            await _queue.SendCommand(new FooMsg());
            while (!await _subscriber.TryHandleNext()) { }

            _barHandler.Log.Should().BeEmpty();
        }

        [Fact]
        public async Task HandleNext_Should_Process_Message_Received_After_The_Call()
        {
            var msg = new FooMsg { Text = "some text" };

            var handleNextTask = _subscriber.HandleNext();
            await _queue.SendCommand(msg);
            await handleNextTask;

            _fooHandler.Log.Single().Text.Should().Be(msg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Not_Process_Message_Received_After_The_Call()
        {
            var msg = new FooMsg { Text = "some text" };

            using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.Zero);
            var tryHandleNextTask = _subscriber.TryHandleNext(cancellationTokenSource.Token);
            await _queue.SendCommand(msg);
            (await tryHandleNextTask).Should().BeFalse();

            await _subscriber.HandleNext(); // To remove the message from the queue so that the next test can pass.
        }

        [Fact]
        public async Task HandleNext_Should_Skip_Invalid_Messages()
        {
            var invalidMsg = new NotHandledMessage { Text = "just a string that has no handlers" };
            var validMsg = new FooMsg { Text = "some text" };

            await _queue.SendCommand(invalidMsg);
            await _queue.SendCommand(validMsg);

            await _subscriber.HandleNext();

            _fooHandler.Log.Single().Text.Should().Be(validMsg.Text);
        }

        [Fact]
        public async Task TryHandleNext_Should_Skip_Invalid_Messages()
        {
            var invalidMsg = new NotHandledMessage { Text = "just a string that has no handlers" };
            var validMsg = new FooMsg { Text = "some text" };

            await _queue.SendCommand(invalidMsg);
            await _queue.SendCommand(validMsg);

            while (!await _subscriber.TryHandleNext()) { }

            _fooHandler.Log.Single().Text.Should().Be(validMsg.Text);
        }

        [Fact]
        public async Task Send_Should_Broadcast_To_More_Than_One_Handler()
        {
            await _queue.SendCommand(new MultiMsg());
            (await _subscriber.TryHandleNext()).Should().BeTrue();
            _handler1.Log.Should().HaveCount(1);
            _handler2.Log.Should().HaveCount(1);
        }
    }
}
