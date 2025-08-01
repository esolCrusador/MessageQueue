using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.RabbitMQ;
using EsoTech.MessageQueue.RabbitMQ.Services;
using EsoTech.MessageQueue.Testing;
using EsoTech.MessageQueue.Tests.EventHandlers;
using EsoTech.MessageQueue.Tests.Messages;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EsoTech.MessageQueue.Tests
{

    public abstract class DeadLetterQueueFacts : IAsyncLifetime
    {
        private const int MaxDeliveryCount = 15;
        private readonly ServiceProvider _serviceProvider;
        private FooEventDelegateHandler DelegateHandler => _serviceProvider.GetRequiredService<FooEventDelegateHandler>();
        public DeadLetterQueueFacts(IServiceCollection services)
        {
            services.AddLogging();
            services.AddSingleton(new ConfigurationBuilder().AddInMemoryCollection().Build());
            services.AddSingleton<IConfiguration>(sp => sp.GetRequiredService<IConfigurationRoot>());
            _serviceProvider = services.BuildServiceProvider();
        }

        [Trait("Category", "Slow")]
        [Collection(nameof(RabbitMqCollectionCollection))]
        public class SlowRabbitMQ : DeadLetterQueueFacts
        {
            public SlowRabbitMQ(RabbitMqTestFixture rabbitMqTestFixture) : base(
                new ServiceCollection().AddMessageQueue((config, sp) =>
                {
                    config.HandleRealtime = true;
                    config.AckTimeout = TimeSpan.FromMicroseconds(1);
                })
                .AddRabbitMq((opts, cfg) =>
                {
                    rabbitMqTestFixture.Configure(opts, cfg);
                    opts.MaxDeliveryCount = MaxDeliveryCount;
                })
                .AddEventMessageHandler<FooEventDelegateHandler>()
            )
            {
            }
        }

        public async Task InitializeAsync()
        {
            if (_serviceProvider.GetService<FakeMessageQueueInitializer>() == null)
                await _serviceProvider.GetRequiredService<IMessageConsumer>().Initialize(default);
        }

        public async Task DisposeAsync()
        {
            await _serviceProvider.DisposeAsync();
        }

        [Fact]
        public async Task Should_Retry_Only_Max_Redelivery_Count_Times()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;

                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await Wait(() => delivers == MaxDeliveryCount);

            var management = _serviceProvider.GetRequiredService<RabbitMqManagement>();
            await Wait(async () => (await management.GetDeadletterQueueStats(default)).Any(q => q.MessagesCount == 1));
        }

        [Fact]
        public async Task Should_Readd_Message_To_Queue()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await Wait(() => delivers == MaxDeliveryCount);

            var mq = _serviceProvider.GetRequiredService<RabbitMqMessageQueue>();
            await Wait(async () => (await mq.RepublishErrorQueues(null, default)) > 0);

            await Wait(() => delivers == MaxDeliveryCount * 2);
        }


        [Fact]
        public async Task Should_Celanup_Message_Queue()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await Wait(() => delivers == MaxDeliveryCount);

            var mq = _serviceProvider.GetRequiredService<RabbitMqMessageQueue>();
            await Wait(async () => (await mq.CleanupErrorQueues(null, default)) > 0);

            var management = _serviceProvider.GetRequiredService<RabbitMqManagement>();
            await Wait(async () => (await management.GetDeadletterQueueStats(default)).Any(q => q.MessagesCount == 0));
        }

        [Fact]
        public async Task Should_Handle_Message_From_DeadLetter()
        {
            int delivers = 0;
            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                throw new Exception();
            };
            await _serviceProvider.GetRequiredService<IMessageQueue>().SendEvent(new FooMsg());

            await Wait(() => delivers == MaxDeliveryCount);

            DelegateHandler.Handler = (ev, cancellation) =>
            {
                delivers++;
                return Task.CompletedTask;
            };

            var mq = _serviceProvider.GetRequiredService<RabbitMqMessageQueue>();
            await Wait(async () => (await mq.RepublishErrorQueues(null, default)) > 0);

            await Wait(() => delivers == MaxDeliveryCount + 1);
        }

        private async ValueTask Wait(Func<ValueTask<bool>> condition, TimeSpan? timeout = default)
        {
            timeout ??= Debugger.IsAttached ? TimeSpan.FromMinutes(1) : TimeSpan.FromSeconds(5);
            var endDate = DateTimeOffset.UtcNow + timeout;

            while (DateTimeOffset.UtcNow < endDate)
            {
                if (await condition())
                    return;

                await Task.Delay(timeout.Value / 50);
            }

            throw new TimeoutException($"Timeout {timeout} has passed");
        }

        private ValueTask Wait(Func<bool> condition, TimeSpan? timeout = default)
        {
            return Wait(() => new ValueTask<bool>(condition()), timeout);
        }
    }
}
