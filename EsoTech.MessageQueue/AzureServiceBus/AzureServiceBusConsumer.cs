using Newtonsoft.Json;
using OpenTracing;
using OpenTracing.Propagation;
using OpenTracing.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Prometheus;
using EsoTech.MessageQueue.Abstractions;
using System.Reactive.Subjects;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Linq.Expressions;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal sealed class AzureServiceBusConsumer : IMessageConsumer
    {
        private Subject<Unit> _handled = new Subject<Unit>();
        private static readonly Counter ExecutedMessages =
            Metrics.CreateCounter("service_bus_executed_messages_amount", "Currently executed amount of messages", "event_name", "queue_name");

        private static readonly Counter FailedMessages =
            Metrics.CreateCounter("service_bus_failed_messages_amount", "Current failed amount of messages", "event_name", "queue_name");

        private static readonly Histogram MessagesDurations =
            Metrics.CreateHistogram("service_bus_messages_durations_seconds", "Durations of performed messages", "event_name", "queue_name");

        private Task WaitBeforeHandle
        {
            get
            {
                if (_messageQueueConfiguration.HandleRealtime)
                    return Task.CompletedTask;

                return _waitBeforeHandle.Take(1).ToTask();
            }
        }

        private IObservable<Unit> _waitBeforeHandle;
        private Subject<Unit> _handleNext;

        private readonly IEnumerable<IMessageHandler> _handlers;
        private readonly TracerFactory _tracerFactory;
        private readonly MessageQueueConfiguration _messageQueueConfiguration;
        private readonly ServiceBusClient _serviceBusClient;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;
        private readonly ILogger<AzureServiceBusConsumer> _logger;

        private ILookup<Guid, HandlerByMessageTypeEntry> _handlersByMessageType;
        private IList<ServiceBusProcessor> _processors;
        private bool _initialized = false;

        private ITracer Tracer => _tracerFactory?.Tracer;

        public AzureServiceBusConsumer(
            IEnumerable<IMessageHandler> handlers,
            TracerFactory tracerFactory,
            MessageQueueConfiguration messageQueueConfiguration,
            AzureServiceBusNamingConvention namingConvention,
            AzureServiceBusClientHolder serviceBusClientHolder,
            MessageSerializer messageSerializer,
            ILogger<AzureServiceBusConsumer> logger)
        {
            _handlers = handlers;
            _tracerFactory = tracerFactory;
            _messageQueueConfiguration = messageQueueConfiguration;
            _serviceBusClient = serviceBusClientHolder.Instance;
            _namingConvention = namingConvention;
            _messageSerializer = messageSerializer;
            _logger = logger;

            if (!_messageQueueConfiguration.HandleRealtime)
            {
                _handleNext = new Subject<Unit>();
                _waitBeforeHandle = _handleNext.PostponeNotHandled();
            }
        }

        public AzureServiceBusConsumer(
            IEnumerable<IMessageHandler> handlers,
            MessageQueueConfiguration messageQueueConfiguration,
            AzureServiceBusNamingConvention namingConvention,
            AzureServiceBusClientHolder serviceBusClientHolder,
            MessageSerializer messageSerializer,
            ILogger<AzureServiceBusConsumer> logger)
            : this(handlers,
                 null,
                 messageQueueConfiguration,
                 namingConvention,
                 serviceBusClientHolder,
                 messageSerializer,
                 logger)
        {
        }

        public async Task Initialize(CancellationToken cancellation)
        {
            _logger.LogInformation("Initializing service bus consumer.");
            var methodName = nameof(IMessageHandler<int>.Handle);

            var entries = _handlers
                .SelectMany(h =>
                {
                    var type = h.GetType();
                    var interfaceName = typeof(IMessageHandler<>).Name;
                    var interfaces = type.GetInterfaces()
                        .Where(t => t.Name == interfaceName)
                        .ToList();

                    _logger.LogInformation(
                        $"Message handler: {type.FullName} targets: {string.Join(",", interfaces.Select(x => x.GenericTypeArguments[0]))}");

                    return interfaces.Select(i => new HandlerByMessageTypeEntry(h, i, methodName));
                })
                .ToArray();

            _handlersByMessageType = entries.ToLookup(x => x.MessageType.GUID);
            _processors = await Subscribe(entries, cancellation);
            _initialized = true;

            _logger.LogInformation("Message queue initialized.");
        }

        public Task HandleNext(CancellationToken cancellation)
        {
            var handledTask = _handled.Take(1).ToTask(cancellation);

            _handleNext?.OnNext(Unit.Default);

            return handledTask;
        }

        public async Task<bool> TryHandleNext(CancellationToken cancellationToken = default)
        {
            CancellationTokenSource cancellationTokenSource = null;
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
            if (!_initialized)
                return;

            var tasks = _processors.Select(x => x.DisposeAsync().AsTask());

            await Task.WhenAll(tasks);

            _handled.OnCompleted();
            _handled.Dispose();
            _handleNext?.OnCompleted();
            _handleNext?.Dispose();
        }

        private async Task<IList<ServiceBusProcessor>> Subscribe(HandlerByMessageTypeEntry[] handlerInfos, CancellationToken cancellation)
        {
            var processors = new List<ServiceBusProcessor>();
            var byTopic = handlerInfos.GroupBy(x => _namingConvention.GetTopicName(x.MessageType));

            foreach (var group in byTopic)
            {
                var topicName = group.Key;
                var subscriptions = group
                    .Select(x => _namingConvention.GetSubscriptionName(x.MessageType, x.HandlerType))
                    .Distinct();

                foreach (var subscriptionName in subscriptions)
                {
                    _logger.LogInformation("Subscribing to topic {TopicName}, subscription {SubscriptionName}", topicName, subscriptionName);

                    var processor = _serviceBusClient.CreateProcessor(topicName, subscriptionName,
                        new ServiceBusProcessorOptions
                        {
                            ReceiveMode = ServiceBusReceiveMode.PeekLock,
                            MaxConcurrentCalls = _messageQueueConfiguration.MaxConcurrentMessages,
                            MaxAutoLockRenewalDuration = TimeSpan.Zero,
                            AutoCompleteMessages = false
                        });

                    processor.ProcessMessageAsync += ProcessMessage;
                    processor.ProcessErrorAsync += ProcessError;

                    await processor.StartProcessingAsync(cancellation);

                    processors.Add(processor);
                }
            }

            return processors;
        }

        private async Task ProcessMessage(ProcessMessageEventArgs args)
        {
            if (!_messageSerializer.TryDeserialize(args.Message.Body, out var message))
            {
                await args.CompleteMessageAsync(args.Message);
                _logger.LogError("Could not deserialize message {Message}", args.Message);

                return;
            }

            var payloadType = message.Payload.GetType();
            var eventName = payloadType.Name;
            var topicName = _namingConvention.GetTopicName(payloadType);

            try
            {
                var handlers = _handlersByMessageType[payloadType.GUID].ToList();
                if (handlers.Count == 0)
                {
                    _logger.LogError("Processed message with no handlers, subscription filters are not set up properly: {Sequence}, {PayloadType}, {MessageBody}.",
                        args.Message.EnqueuedSequenceNumber, payloadType, JsonConvert.SerializeObject(message.Payload));

                    await args.CompleteMessageAsync(args.Message, args.CancellationToken);

                    return;
                }

                await WaitBeforeHandle;

                _logger.LogInformation("Processing Message {Sequence}, {PayloadType}, {MessageBody}",
                    args.Message.EnqueuedSequenceNumber,
                    payloadType,
                    JsonConvert.SerializeObject(message.Payload));

                using var cancellationTokenSource = new CancellationTokenSource(_messageQueueConfiguration.AckTimeoutMilliseconds);
                using (MessagesDurations.WithLabels(eventName, topicName).NewTimer())
                {
                    foreach (var handlerInfo in handlers)
                    {
                        if (Tracer == null)
                        {
                            await handlerInfo.Handle.Value(message.Payload, cancellationTokenSource.Token);

                            continue;
                        }

                        var msgType = message.Payload.GetType().Name;
                        var currentActive = Tracer.ActiveSpan;
                        var spanContext = Tracer.Extract(BuiltinFormats.TextMap, new TextMapExtractAdapter(message.Headers));

                        try
                        {
                            using (Tracer.BuildSpan($"{msgType}")
                                .WithTag(Tags.Component, "MessageQueue")
                                .WithTag(Tags.SpanKind, Tags.SpanKindConsumer)
                                .WithTag("mq.message", JsonConvert.SerializeObject(message.Payload))
                                .AsChildOf(spanContext)
                                .StartActive(true))

                                try
                                {
                                    await handlerInfo.Handle.Value(message.Payload, cancellationTokenSource.Token);
                                }
                                catch (Exception e)
                                {
                                    Tracer.ActiveSpan?.Log(new Dictionary<string, object>
                                    {
                                        {"type", e.GetType().Name}, {"message", e.Message}, {"stackTrace", e.StackTrace},
                                        {"data", e.Data}
                                    });

                                    Tracer.ActiveSpan?.SetTag("error", true);

                                    throw;
                                }
                        }
                        finally
                        {
                            Tracer.ScopeManager.Activate(currentActive, true);
                        }
                    }
                }

                _handled.OnNext(Unit.Default);
                await args.CompleteMessageAsync(args.Message, args.CancellationToken);

                ExecutedMessages.WithLabels(eventName, topicName).Inc();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ProcessMessage error");

                FailedMessages.WithLabels(eventName, topicName).Inc();
            }
        }

        private Task ProcessError(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception,
                "Error while processing messages from {SubscriptionName}, source: {ErrorSource}", args.EntityPath, args.ErrorSource);

            return Task.CompletedTask;
        }



        private class HandlerByMessageTypeEntry
        {
            public Type MessageType { get; }

            public Lazy<Func<object, CancellationToken, Task>> Handle { get; }

            public Type HandlerType { get; }

            public HandlerByMessageTypeEntry(IMessageHandler instance, Type interfaceType, string methodName)
            {
                MessageType = interfaceType.GetGenericArguments()[0];
                Handle = new Lazy<Func<object, CancellationToken, Task>>(
                    () => HandlerExtensions.CreateHandleDelegate(instance, interfaceType, methodName)
                );
                HandlerType = instance.GetType();
            }
        }
    }
}
