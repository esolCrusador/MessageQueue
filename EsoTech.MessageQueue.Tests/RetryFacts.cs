using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using EsoTech.MessageQueue.Testing;
using EsoTech.MessageQueue.Tests.EventHandlers;
using EsoTech.MessageQueue.Tests.Messages;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{
    public abstract class RetryFacts : IAsyncLifetime
    {
        private readonly ServiceProvider _serviceProvier;
        private readonly IMessageConsumer _subscriber;
        private readonly FooEventHandler _fooHandler;
        private readonly FooEventDelegateHandler _fooDelegateHandler;
        private readonly AzureServiceBusManager? _azureServiceBusManager;
        private readonly IMessageQueue _queue;

        //[Trait("Category", "Fast")]
        //public sealed class Fast : RetryFacts
        //{
        //    public Fast() : base(CreateServiceProvider())
        //    {
        //    }

        //    public static ServiceProvider CreateServiceProvider()
        //    {
        //        var serviceCollection = ConfigureCommonServices(new ServiceCollection());
        //        serviceCollection.AddFakeMessageQueue();

        //        var serviceProvider = serviceCollection.BuildServiceProvider();

        //        return serviceProvider;
        //    }
        //}

        [Trait("Category", "Slow")]
        public sealed class Slow : RetryFacts
        {
            public Slow() : base(ConfigureCommonServices(new ServiceCollection())
                .AddSingleton<IConfiguration>(new ConfigurationBuilder().AddEnvironmentVariables().Build())
                .SuppressContinuousPolling()
                .AddMessageQueue(opts => opts.AckTimeout = TimeSpan.FromSeconds(1))
                .AddAzureServiceBusMessageQueue(opts => opts.ConnectionStringName = "TestServiceBusConnectionString")
                .BuildServiceProvider()
            )
            {
            }
        }

        private static IServiceCollection ConfigureCommonServices(IServiceCollection services)
        {
            return services.AddLogging()
                    .AddEventMessageHandler<FooEventDelegateHandler>()
                    .AddEventMessageHandler<FooEventHandler>();
        }

        private RetryFacts(ServiceProvider serviceProvider)
        {
            _serviceProvier = serviceProvider;

            _subscriber = serviceProvider.GetRequiredService<IMessageConsumer>();
            _fooHandler = serviceProvider.GetRequiredService<FooEventHandler>();
            _fooDelegateHandler = serviceProvider.GetRequiredService<FooEventDelegateHandler>();
            _azureServiceBusManager = serviceProvider.GetService<AzureServiceBusManager>();
            _queue = serviceProvider.GetRequiredService<IMessageQueue>();
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
            _fooDelegateHandler.Handler = (_, _) => Task.FromException(new Exception());
            await _queue.SendEvent(new FooMsg { Text = text });
            using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                (await _subscriber.TryHandleNext(cancellation.Token)).Should().BeFalse();

            _fooHandler.Log.Should().BeEquivalentTo(new[] { new FooMsg { Text = text } });
            _fooDelegateHandler.Log.Should().BeEmpty();

            _fooDelegateHandler.ResetHandler();
            (await _subscriber.TryHandleNext()).Should().BeTrue();
            _fooDelegateHandler.Log.Should().BeEquivalentTo(new[] { new FooMsg { Text = text } });
            _fooHandler.Log.Should().BeEquivalentTo(new[] { new FooMsg { Text = text } });
        }

        [Fact]
        public async Task Should_Handle_Only_Once_In_Case_Of_Failure()
        {
            _fooDelegateHandler.Handler = (_, _) => Task.FromException(new Exception());
            await _queue.SendEvent(new FooMsg { Text = "123" });
            using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                (await _subscriber.TryHandleNext(cancellation.Token)).Should().BeFalse();

            _fooHandler.Log.Should().BeEquivalentTo(new[] { new FooMsg { Text = "123" } });
            _fooDelegateHandler.Log.Should().BeEmpty();

            using (var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
                (await _subscriber.TryHandleNext(cancellation.Token)).Should().BeFalse();

            _fooHandler.Log.Should().BeEquivalentTo(new[] { new FooMsg { Text = "123" } });
            _fooDelegateHandler.Log.Should().BeEmpty();

            _fooDelegateHandler.ResetHandler();
            (await _subscriber.TryHandleNext()).Should().BeTrue();
            _fooDelegateHandler.Log.Should().BeEquivalentTo(new[] { new FooMsg { Text = "123" } });
            _fooHandler.Log.Should().BeEquivalentTo(new[] { new FooMsg { Text = "123" } });
        }
    }
}
