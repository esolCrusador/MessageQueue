using Azure.Messaging.ServiceBus;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTracing;
using OpenTracing.Propagation;
using OpenTracing.Tag;
using Prometheus;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal sealed class AzureServiceBusConsumer : IMessageConsumer
    {
        private static readonly NoopDisposable _defaultDisposable = new NoopDisposable();

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
                if (_messageQueueConfiguration.HandleRealtime || _waitBeforeHandle == null)
                    return Task.CompletedTask;

                return _waitBeforeHandle.Take(1).ToTask();
            }
        }

        private IObservable<Unit>? _waitBeforeHandle;
        private Subject<Unit>? _handleNext;

        private readonly IEnumerable<IEventMessageHandler> _eventHandlers;
        private readonly IEnumerable<ICommandMessageHandler> _commandHandlers;
        private readonly TracerFactory? _tracerFactory;
        private readonly MessageQueueConfiguration _messageQueueConfiguration;
        private readonly ServiceBusClient _serviceBusClient;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;
        private readonly AzureServiceBusManager _azureServiceBusManager;
        private readonly ILogger<AzureServiceBusConsumer> _logger;
        private readonly ConcurrentDictionary<string, HandlerByMessageTypeEntry[]> _notHandledMessages = new ConcurrentDictionary<string, HandlerByMessageTypeEntry[]>();
        private IList<ServiceBusProcessor>? _processors;
        private bool _initialized = false;

        private ITracer? Tracer => _tracerFactory?.Tracer;

        public AzureServiceBusConsumer(
            IEnumerable<IEventMessageHandler> eventHandlers,
            IEnumerable<ICommandMessageHandler> commandHandlers,
            TracerFactory? tracerFactory,
            IOptions<MessageQueueConfiguration> messageQueueConfiguration,
            AzureServiceBusNamingConvention namingConvention,
            AzureServiceBusClientHolder serviceBusClientHolder,
            MessageSerializer messageSerializer,
            AzureServiceBusManager azureServiceBusManager,
            ILogger<AzureServiceBusConsumer> logger)
        {
            _eventHandlers = eventHandlers;
            _commandHandlers = commandHandlers;
            _tracerFactory = tracerFactory;
            _messageQueueConfiguration = messageQueueConfiguration.Value;
            _serviceBusClient = serviceBusClientHolder.Instance;
            _namingConvention = namingConvention;
            _messageSerializer = messageSerializer;
            _azureServiceBusManager = azureServiceBusManager;
            _logger = logger;

            if (!_messageQueueConfiguration.HandleRealtime)
            {
                _handleNext = new Subject<Unit>();
                _waitBeforeHandle = _handleNext.PostponeNotHandled();
            }
        }

        public AzureServiceBusConsumer(
            IEnumerable<IEventMessageHandler> eventHandlers,
            IEnumerable<ICommandMessageHandler> commandHandlers,
            IOptions<MessageQueueConfiguration> messageQueueConfiguration,
            AzureServiceBusNamingConvention namingConvention,
            AzureServiceBusClientHolder serviceBusClientHolder,
            MessageSerializer messageSerializer,
            AzureServiceBusManager azureServiceBusManager,
            ILogger<AzureServiceBusConsumer> logger)
            : this(eventHandlers, commandHandlers,
                 null,
                 messageQueueConfiguration,
                 namingConvention,
                 serviceBusClientHolder,
                 messageSerializer,
                 azureServiceBusManager,
                 logger)
        {
        }

        public async Task Initialize(CancellationToken cancellation)
        {
            var sw = Stopwatch.StartNew();
            _logger.LogInformation("Initializing service bus consumer.");
            const string methodName = "Handle";

            var eventHandlers = _eventHandlers
                .SelectMany(h =>
                {
                    var type = h.GetType();
                    var interfaceName = typeof(IEventMessageHandler<>).Name;
                    var interfaces = type.GetInterfaces()
                        .Where(t => t.Name == interfaceName)
                        .ToList();
                    var longRunningInterface = typeof(ILongRunningHandler<>).Name;

                    _logger.LogInformation(
                        $"Message handler: {type.FullName} targets: {string.Join(",", interfaces.Select(x => x.GenericTypeArguments[0]))}");

                    return interfaces.Select(i => new HandlerByMessageTypeEntry(h, i, methodName));
                })
                .ToArray();
            var commandHandlers = _commandHandlers
                .SelectMany(h =>
                {
                    var type = h.GetType();
                    var interfaceName = typeof(ICommandMessageHandler<>).Name;
                    var interfaces = type.GetInterfaces()
                        .Where(t => t.Name == interfaceName)
                        .ToList();

                    _logger.LogInformation(
                        $"Message handler: {type.FullName} targets: {string.Join(",", interfaces.Select(x => x.GenericTypeArguments[0]))}");

                    return interfaces.Select(i => new HandlerByMessageTypeEntry(h, i, methodName));
                })
                .ToArray();

            var eventProcessors = await SubscribeOnEvents(eventHandlers, cancellation);
            var commandProcessors = await SubscribeOnCommands(commandHandlers, cancellation);
            _processors = eventProcessors.Concat(commandProcessors).ToList();
            _initialized = true;

            _logger.LogInformation("Message queue initialized. {Milliseconds} milliseconds", sw.ElapsedMilliseconds);
        }

        public Task HandleNext(CancellationToken cancellation)
        {
            var handledTask = _handled.Take(1).ToTask(cancellation);

            _handleNext?.OnNext(Unit.Default);

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
            if (!_initialized)
                return;

            var tasks = _processors.Select(x => x.DisposeAsync().AsTask());

            await Task.WhenAll(tasks);

            _handled.OnCompleted();
            _handled.Dispose();
            _handleNext?.OnCompleted();
            _handleNext?.Dispose();
        }

        private async Task<IList<ServiceBusProcessor>> SubscribeOnEvents(HandlerByMessageTypeEntry[] handlerInfos, CancellationToken cancellation)
        {
            var processors = new List<ServiceBusProcessor>();
            var byTopic = handlerInfos.GroupBy(x => _namingConvention.GetTopicName(x.MessageType));

            foreach (var group in byTopic)
            {
                var topicName = group.Key;
                var subscriptions = group
                    .Select(x => new KeyValuePair<string, HandlerByMessageTypeEntry>(_namingConvention.GetSubscriptionName(x.MessageType, x.HandlerType), x))
                    .GroupBy(kvp => kvp.Key, kvp => kvp.Value)
                    .ToList();

                foreach (var subscriptionMessages in subscriptions)
                {
                    var subscriptionName = subscriptionMessages.Key;
                    await _azureServiceBusManager.UpdateSubscription(topicName, subscriptionName, subscriptionMessages.Select(m => m.MessageType));
                    _logger.LogInformation("Subscribing to topic {TopicName}, subscription {SubscriptionName}", topicName, subscriptionName);

                    var processor = _serviceBusClient.CreateProcessor(topicName, subscriptionName,
                        new ServiceBusProcessorOptions
                        {
                            ReceiveMode = ServiceBusReceiveMode.PeekLock,
                            MaxConcurrentCalls = _messageQueueConfiguration.MaxConcurrentMessages,
                            MaxAutoLockRenewalDuration = TimeSpan.Zero,
                            AutoCompleteMessages = false
                        });
                    var handlers = subscriptionMessages.GroupBy(m => m.MessageType).ToDictionary(g => g.Key, g => g.ToArray());

                    processor.ProcessMessageAsync += arguments => ProcessMessage(handlers, arguments);
                    processor.ProcessErrorAsync += ProcessError;

                    await processor.StartProcessingAsync(cancellation);

                    processors.Add(processor);
                }
            }

            return processors;
        }

        private async Task<IList<ServiceBusProcessor>> SubscribeOnCommands(HandlerByMessageTypeEntry[] handlerInfos, CancellationToken cancellation)
        {
            var processors = new List<ServiceBusProcessor>();
            var byQueue = handlerInfos.GroupBy(x => _namingConvention.GetQueueName(x.MessageType));

            foreach (var group in byQueue)
            {
                var queueName = group.Key;

                await _azureServiceBusManager.UpdateQueue(queueName);
                _logger.LogInformation("Subscribing to queue {QueueName}", queueName);

                var processor = _serviceBusClient.CreateProcessor(queueName,
                    new ServiceBusProcessorOptions
                    {
                        ReceiveMode = ServiceBusReceiveMode.PeekLock,
                        MaxConcurrentCalls = _messageQueueConfiguration.MaxConcurrentMessages,
                        MaxAutoLockRenewalDuration = TimeSpan.Zero,
                        AutoCompleteMessages = false
                    });
                var handlers = group.GroupBy(mh => mh.MessageType).ToDictionary(g => g.Key, g => g.ToArray());

                processor.ProcessMessageAsync += arguments => ProcessMessage(handlers, arguments);
                processor.ProcessErrorAsync += ProcessError;

                await processor.StartProcessingAsync(cancellation);

                processors.Add(processor);
            }

            return processors;
        }


        private async Task ProcessMessage(IReadOnlyDictionary<Type, HandlerByMessageTypeEntry[]> handlersByMessageType, ProcessMessageEventArgs args)
        {
            if (!_messageSerializer.TryDeserialize(args.Message.Body, out var message))
            {
                await args.CompleteMessageAsync(args.Message);
                _logger.LogError("Could not deserialize message {Message}", args.Message);

                return;
            }

            var payloadType = (message?.Payload ?? throw new ArgumentException("No payload")).GetType();
            var eventName = payloadType.Name;
            var topicName = _namingConvention.GetTopicName(payloadType);
            var serializedMessage = _messageSerializer.SerializeToString(message.Payload, payloadType);

            try
            {
                if (args.Message.DeliveryCount > 0 && _notHandledMessages.TryGetValue(args.Message.MessageId, out var handlers))
                    _notHandledMessages.Remove(args.Message.MessageId, out _);
                else if (!handlersByMessageType.TryGetValue(payloadType, out handlers))
                {
                    _logger.LogError("Processed message with no handlers, subscription filters are not set up properly: {Sequence}, {PayloadType}, {MessageBody}.",
                        args.Message.EnqueuedSequenceNumber, payloadType, serializedMessage);

                    await args.CompleteMessageAsync(args.Message, args.CancellationToken);

                    return;
                }

                await WaitBeforeHandle;

                _logger.LogInformation("Processing Message {Sequence}, {PayloadType}, {MessageBody}",
                    args.Message.EnqueuedSequenceNumber,
                    payloadType,
                    serializedMessage);

                using (MessagesDurations.WithLabels(eventName, topicName).NewTimer())
                {
                    int failed = 0;
                    var handlerTasks = new List<KeyValuePair<HandlerByMessageTypeEntry, Task>>(handlers.Length);
                    foreach (var handlerInfo in handlers)
                    {
                        var task = ExecuteHandler(args, message, serializedMessage, handlerInfo);
                        handlerTasks.Add(new KeyValuePair<HandlerByMessageTypeEntry, Task>(handlerInfo, task));

                        try
                        {
                            await task;
                        }
                        catch
                        {
                            failed++;
                        }
                    }

                    if (failed > 0)
                    {
                        _notHandledMessages[args.Message.MessageId] = handlerTasks
                                .Where(kvp => kvp.Value.IsFaulted)
                                .Select(kvp => kvp.Key)
                                .ToArray();

                        if (failed == 1)
                        {
                            var ex = handlerTasks.Single(ht => ht.Value.IsFaulted).Value.Exception;
                            if (ex.InnerExceptions.Count == 1)
                                throw ex.InnerExceptions[0];

                            throw ex;
                        }
                        else
                        {
                            throw new AggregateException(handlerTasks.Where(ht => ht.Value.IsFaulted)
                                .SelectMany(ht => ht.Value.Exception.InnerExceptions)
                            );
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

        private async Task ExecuteHandler(ProcessMessageEventArgs args, Message message, string serializedMessage, HandlerByMessageTypeEntry handlerInfo)
        {
            if (message.Payload == null)
                throw new ArgumentException("Null message payload");

            var timeout = handlerInfo.GetTimeout.Value?.Invoke(message.Payload);
            using IDisposable resumeTimer = timeout.HasValue
                ? Observable.Interval(_messageQueueConfiguration.ResumeLockPeriod)
                    .Timeout(timeout.Value)
                    .Select(_ => Observable.FromAsync(cancellation => args.RenewMessageLockAsync(args.Message, cancellation)))
                    .Concat()
                    .Subscribe()
                : _defaultDisposable;

            if (Tracer == null)
            {
                await handlerInfo.Handle.Value(message.Payload, args.CancellationToken);

                return;
            }

            var msgType = message.Payload.GetType().Name;
            var currentActive = Tracer.ActiveSpan;
            var spanContext = Tracer.Extract(BuiltinFormats.TextMap, new TextMapExtractAdapter(message.Headers));

            try
            {
                using (Tracer.BuildSpan($"{msgType}")
                    .WithTag(Tags.Component, "MessageQueue")
                    .WithTag(Tags.SpanKind, Tags.SpanKindConsumer)
                    .WithTag("mq.message", serializedMessage)
                    .AsChildOf(spanContext)
                    .StartActive(true))

                    try
                    {
                        await handlerInfo.Handle.Value(message.Payload, args.CancellationToken);
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

        private async Task ProcessError(ProcessErrorEventArgs args)
        {
            _logger.LogError(args.Exception, "Error while processing messages from {SubscriptionName}, source: {ErrorSource}", args.EntityPath, args.ErrorSource);

            if (args.Exception.IsNotFound())
                await _azureServiceBusManager.Reset();
        }



        private class HandlerByMessageTypeEntry
        {
            public Type MessageType { get; }
            public Lazy<Func<object, TimeSpan?>?> GetTimeout { get; }
            public Lazy<Func<object, CancellationToken, Task>> Handle { get; }
            public Type HandlerType { get; }

            public HandlerByMessageTypeEntry(object instance, Type interfaceType, string methodName)
            {
                MessageType = interfaceType.GetGenericArguments()[0];
                Handle = new Lazy<Func<object, CancellationToken, Task>>(
                    () => HandlerExtensions.CreateHandleDelegate(instance, interfaceType, methodName)
                );
                GetTimeout = new Lazy<Func<object, TimeSpan?>?>(
                    () => HandlerExtensions.CreateGetTimeoutDelegate(instance, MessageType)
                );
                HandlerType = instance.GetType();
            }
        }

        private class NoopDisposable : IDisposable
        {
            public void Dispose() { }
        }
    }
}
