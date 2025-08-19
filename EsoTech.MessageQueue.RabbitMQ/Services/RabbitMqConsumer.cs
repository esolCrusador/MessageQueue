using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Reactive;
using System.Reactive.Subjects;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using EsoTech.MessageQueue.Extensions;
using EsoTech.MessageQueue.RabbitMQ.Serialization;
using System.Threading.Channels;

namespace EsoTech.MessageQueue.RabbitMQ.Services
{
    public class RabbitMqConsumer : IMessageConsumer
    {
        private readonly RabbitMQClient _rabbitMQClient;
        private readonly RabbitMqQueueFactory _queueFactory;
        private readonly NamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;
        private readonly RabbitMQConfiguration _options;
        private readonly MessageQueueConfiguration _messagingOptions;
        private readonly IReadOnlyCollection<IEventMessageHandler> _eventHandlers;
        private readonly IReadOnlyCollection<ICommandMessageHandler> _commandHandlers;
        private readonly CancellationTokenSource _stopToken;
        private readonly ILogger _logger;

        private Subject<Unit>? _handled;
        private Subject<Unit>? _handleNext;
        private IObservable<Unit>? _waitBeforeHandle;
        private Task WaitBeforeHandle
        {
            get
            {
                if (_messagingOptions.HandleRealtime || _waitBeforeHandle == null)
                    return Task.CompletedTask;

                return _waitBeforeHandle.Take(1).ToTask();
            }
        }

        public RabbitMqConsumer(RabbitMQClient rabbitMQClient, RabbitMqQueueFactory queueFactory, IEnumerable<IEventMessageHandler> eventHandlers, IEnumerable<ICommandMessageHandler> commandHandlers, NamingConvention namingConvention, MessageSerializer messageSerializer, IOptions<RabbitMQConfiguration> options, IOptions<MessageQueueConfiguration> messagingOptions, ILogger<RabbitMqConsumer> logger)
        {
            _rabbitMQClient = rabbitMQClient;
            _queueFactory = queueFactory;
            _eventHandlers = eventHandlers.ToList();
            _commandHandlers = commandHandlers.ToList();
            _namingConvention = namingConvention;
            _messageSerializer = messageSerializer;
            _options = options.Value;
            _messagingOptions = messagingOptions.Value;
            _stopToken = new CancellationTokenSource();
            _logger = logger;

            if (!_messagingOptions.HandleRealtime)
            {
                _handled = new Subject<Unit>();
                _handleNext = new Subject<Unit>();
                _waitBeforeHandle = _handleNext.PostponeNotHandled();
            }
        }

        public Task Initialize(CancellationToken cancellationToken)
        {
            return Task.WhenAll(CreateEventHandlers(cancellationToken), CreateCommandHandlers(cancellationToken));
        }

        private async Task CreateEventHandlers(CancellationToken cancellationToken)
        {
            var handlerTypes = _eventHandlers.ToDictionary(
                h => h,
                h => h.GetType().GetInterfaces().Where(it => it.IsGenericType && it.GetGenericTypeDefinition() == typeof(IEventMessageHandler<>)).ToList()
            );

            var subscriptions = handlerTypes.SelectMany(kvp =>
            {
                var subscriptionName = _namingConvention.GetSubscriptionName(kvp.Key.GetType());

                return kvp.Value.Select(ht =>
                {
                    var messageType = ht.GenericTypeArguments[0];
                    var topicName = _namingConvention.GetTopicName(messageType);

                    return new EventMessageSubscription(
                                        topicName,
                                        $"{topicName}-{subscriptionName}",
                                        _namingConvention.GetRoutingKey(messageType),
                                        messageType,
                                        kvp.Key
                                );
                });
            }).GroupBy(ms => new TopicSubscription(ms.Topic, ms.Subscription))
            .Select(g => new KeyValuePair<TopicSubscription, MessageHandler[]>(g.Key, g.Select(s => new MessageHandler(s.RoutingKey, s.MessageType, s.Handler)).ToArray()));

            await Parallel.ForEachAsync(subscriptions,
                 new ParallelOptions
                 {
                     CancellationToken = cancellationToken,
                     MaxDegreeOfParallelism = _options.InitializationParallelism,
                 },
                 async (kvp, cancellation) =>
                 {
                     var (subscription, messageHandlers) = kvp;
                     await StartConsumerLoop(
                         new SubscriptionFactory(subscription.Subscription, typeof(IEventMessageHandler<>), channel => _rabbitMQClient.CreateEventSubscription(
                             channel,
                             subscription.Topic,
                             subscription.Subscription,
                             messageHandlers.Select(mh => mh.RoutingKey),
                             cancellation)
                         ),
                         messageHandlers,
                         cancellation
                     );
                 });
        }

        private async Task CreateCommandHandlers(CancellationToken cancellationToken)
        {
            var handlerTypes = _commandHandlers.ToDictionary(
                h => h,
                h => h.GetType().GetInterfaces().Where(it => it.IsGenericType && it.GetGenericTypeDefinition() == typeof(ICommandMessageHandler<>)).ToList()
            );

            var subscriptions = handlerTypes.SelectMany(kvp =>
            {
                return kvp.Value.Select(ht =>
                {
                    var messageType = ht.GenericTypeArguments[0];
                    var queueName = _namingConvention.GetQueueName(messageType);

                    return new CommandMessageSubscription(
                                        queueName,
                                        _namingConvention.GetRoutingKey(messageType),
                                        messageType,
                                        kvp.Key
                                );
                });
            }).GroupBy(ms => ms.Queue)
            .Select(g => new KeyValuePair<string, MessageHandler[]>(g.Key, g.Select(s => new MessageHandler(s.RoutingKey, s.MessageType, s.Handler)).ToArray()));

            await Parallel.ForEachAsync(subscriptions,
                new ParallelOptions
                {
                    CancellationToken = cancellationToken,
                    MaxDegreeOfParallelism = _options.InitializationParallelism
                },
                async (kvp, cancellation) =>
                {
                    var (queueName, messageHandlers) = kvp;

                    await StartConsumerLoop(
                        new SubscriptionFactory(queueName, typeof(ICommandMessageHandler<>), channel => _rabbitMQClient.CreateCommandSubscription(
                            channel,
                            queueName,
                            messageHandlers.Select(mh => mh.RoutingKey),
                            cancellation)
                        ),
                        messageHandlers,
                        cancellation
                    );
                });
        }

        private record struct TopicSubscription(string Topic, string Subscription);
        private record struct MessageHandler(string RoutingKey, Type MessageType, object Handler);
        private record struct EventMessageSubscription(string Topic, string Subscription, string RoutingKey, Type MessageType, IEventMessageHandler Handler);
        private record struct CommandMessageSubscription(string Queue, string RoutingKey, Type MessageType, ICommandMessageHandler Handler);
        private record struct SubscriptionFactory(string Name, Type HandlerGenericType, Func<IChannel, Task> Create);

        private Task StartConsumerLoop(SubscriptionFactory subscriptionFactory, IReadOnlyCollection<MessageHandler> handlers, CancellationToken cancellationToken)
        {
            var subscribed = new TaskCompletionSource();
            var _ = ConsumerLoop(subscribed, subscriptionFactory, handlers, cancellationToken);

            return subscribed.Task;
        }
        private async Task ConsumerLoop(TaskCompletionSource subscribed, SubscriptionFactory subscriptionFactory, IReadOnlyCollection<MessageHandler> handlers, CancellationToken cancellationToken)
        {
            var messageHandlers = handlers.GroupBy(h => h.MessageType)
                .ToDictionary(g => g.Key, g => g.Select(h => CreateHandlerDelegate(h.Handler, h.MessageType, subscriptionFactory.HandlerGenericType)).ToList());

            var reconnects = 0;
            IChannel? channel = default;
            using var failureTokenSource = new CancellationTokenSource();
            while (!_stopToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested && !failureTokenSource.IsCancellationRequested)
            {
                try
                {
                    using var restartTokenSource = new CancellationTokenSource();
#pragma warning disable CA2000 // Dispose objects before losing scope
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(_stopToken.Token, cancellationToken, failureTokenSource.Token, restartTokenSource.Token);
#pragma warning restore CA2000 // Dispose objects before losing scope
                    reconnects++;

                    channel = await _rabbitMQClient.CreateChannel();
                    await channel.BasicQosAsync(0, (ushort)int.Min(ushort.MaxValue, _messagingOptions.MaxConcurrentMessages * 2), false);

                    channel.ChannelShutdownAsync += (sender, @event) =>
                    {
                        if (!cts.IsCancellationRequested)
                            restartTokenSource.Cancel();

                        return Task.CompletedTask;
                    };
                    channel.CallbackExceptionAsync += (sender, e) =>
                    {
                        if (!cts.IsCancellationRequested)
                            failureTokenSource.Cancel();

                        return Task.CompletedTask;
                    };

                    await subscriptionFactory.Create(channel);
                    var consumer = new AsyncEventingBasicConsumer(channel);

                    consumer.UnregisteredAsync += (sender, args) =>
                    {
                        if (!cts.IsCancellationRequested)
                            restartTokenSource.Cancel();

                        return Task.CompletedTask;
                    };
                    using var parallelism = new SemaphoreSlim(_messagingOptions.MaxConcurrentMessages);
                    consumer.ReceivedAsync += async (sender, args) =>
                    {
                        await parallelism.WaitAsync(cancellationToken);
                        var _ = HandleMessage(parallelism, channel, subscriptionFactory.Name, args, messageHandlers, cancellationToken)
                            .ContinueWith(task =>
                            {
                                if (task.IsFaulted)
                                    _logger.LogCritical(task.Exception.Flatten().InnerException ?? task.Exception, $"{nameof(HandleMessage)} has failed");
                            });
                    };


                    var startedConsumer = await channel.BasicConsumeAsync(subscriptionFactory.Name, false, consumer);

                    _logger.LogInformation("Started consuming: {ConsumerName} {Callback}", subscriptionFactory.Name, startedConsumer);
                    subscribed.TrySetResult();

                    reconnects = 0;

                    await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Consumer for {Subscription} has been stopped", subscriptionFactory.Name);
                }
                catch (Exception ex)
                {
                    if (reconnects >= _options.MaxConnectionRetries)
                    {
                        _logger.LogError(ex, "Consumer for {Subscription} failed to start and shut down after {ConnectionNumber} unsuccessful retries.", subscriptionFactory.Name, reconnects);
                        await failureTokenSource.CancelAsync();
                    }
                    else
                    {
                        _logger.LogWarning(ex, "Consumer for {Subscription} failed to start.", subscriptionFactory.Name);
                        await Task.Delay(_options.ReconnectTimeout, cancellationToken);
                    }
                }
                finally
                {
                    await (channel?.DisposeAsync() ?? default);
                }
            }
        }

        private async Task HandleMessage(SemaphoreSlim parallelism, IChannel channel, string subscriptionName, BasicDeliverEventArgs args, Dictionary<Type, List<Func<object, CancellationToken, Task>>> messageHandlers, CancellationToken cancellationToken)
        {
            var started = DateTimeOffset.UtcNow;
            string? messageText = null;
            Message? message = null;

            try
            {
                message = _messageSerializer.Deserialize(args.Body.Span);
                messageText = _messageSerializer.SerializeToString(message, typeof(Message));
                _logger.LogInformation("Handling message {Message}", messageText);

                await WaitBeforeHandle;

                foreach (var handler in messageHandlers[message.Payload!.GetType()])
                    await handler(message.Payload, args.CancellationToken);

                await channel.BasicAckAsync(args.DeliveryTag, false);

                _handled?.OnNext(Unit.Default);
                parallelism.Release();
            }
            catch (Exception ex)
            {
                parallelism.Release();
                _logger.LogError(ex, "Consumer failed {Subscrription}", subscriptionName);

                int rediliveryCount = 0;
                if (args.BasicProperties.Headers?.TryGetValue(NamingConvention.RediliveryCountHeader, out var redeliveryCountObj) == true)
                    rediliveryCount = int.Parse(Encoding.UTF8.GetString((byte[])redeliveryCountObj!));
                rediliveryCount++;

                var properties = new BasicProperties(args.BasicProperties)
                {
                    Headers = new Dictionary<string, object?>(args.BasicProperties.Headers ?? new Dictionary<string, object?>()),
                    Persistent = true,
                    DeliveryMode = DeliveryModes.Persistent
                };
                if (rediliveryCount >= _options.MaxDeliveryCount)
                {
                    var deadLetterQueue = await _queueFactory.CreateDeadLetterQueueue(subscriptionName, cancellationToken);

                    properties.Headers[NamingConvention.DeadletterRoutingKeyHeader] = args.RoutingKey;
                    _logger.LogWarning("Sending message {Message} to deadletter queue after {Retries} retries", messageText, rediliveryCount);
                    await channel.BasicPublishAsync(
                        deadLetterQueue,
                        "",
                        true,
                        properties,
                        body: message == null ? args.Body.ToArray() : _messageSerializer.Serialize(message),
                        cancellationToken
                    );
                }
                else
                {
                    var delayBeforeNack = _messagingOptions.AckTimeout - (DateTimeOffset.UtcNow - started);
                    if (delayBeforeNack > TimeSpan.Zero)
                        await Task.Delay(delayBeforeNack, args.CancellationToken);

                    properties.Headers[NamingConvention.RediliveryCountHeader] = Encoding.UTF8.GetBytes(rediliveryCount.ToString());

                    _logger.LogWarning("Republishing message {Message}. Retries: {Retries}", messageText, rediliveryCount);
                    await channel.BasicPublishAsync(
                        subscriptionName,
                        args.RoutingKey,
                        true,
                        properties,
                        body: message == null ? args.Body.ToArray() : _messageSerializer.Serialize(message)
                    );
                }

                await channel.BasicAckAsync(args.DeliveryTag, false);
            }
        }

        private Func<object, CancellationToken, Task> CreateHandlerDelegate(object handler, Type messageType, Type handlerGenericType)
        {
            var message = Expression.Parameter(typeof(object), "message");
            var cancellation = Expression.Parameter(typeof(CancellationToken), "cancellationToken");

            var messageHandlerType = handlerGenericType.MakeGenericType(messageType);
            var messageHandler = Expression.Convert(Expression.Constant(handler), messageHandlerType);
            var method = messageHandlerType.GetMethod("Handle") ?? throw new ArgumentException($"Method Handle not found in IHandler<{messageType.FullName}>");

            return Expression.Lambda<Func<object, CancellationToken, Task>>(
                Expression.Call(messageHandler, method, Expression.Convert(message, messageType), cancellation),
                message,
                cancellation
            ).Compile();
        }

        public Task HandleNext(CancellationToken cancellation)
        {
            if (_messagingOptions.HandleRealtime)
                throw new InvalidOperationException($"To use {nameof(HandleNext)} set {nameof(_messagingOptions.HandleRealtime)} to false");

            var handledTask = _handled!.Take(1).ToTask(cancellation);
            _handleNext!.OnNext(Unit.Default);

            return handledTask;
        }

        public async Task<bool> TryHandleNext(CancellationToken cancellationToken = default)
        {
            CancellationTokenSource? cancellationTokenSource = null;
            if (cancellationToken == default)
            {
                cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                cancellationToken = cancellationTokenSource.Token;
            }

            try
            {
                await HandleNext(cancellationToken);
                return true;
            }
            catch (TaskCanceledException)
            {
                return false;
            }
            finally
            {
                cancellationTokenSource?.Dispose();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _stopToken.CancelAsync();
            _stopToken.Dispose();

            _handled?.OnCompleted();
            _handled?.Dispose();
            _handleNext?.OnCompleted();
            _handleNext?.Dispose();
        }
    }
}
