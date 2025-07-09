using Azure.Messaging.ServiceBus;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.Logging;
using OpenTracing;
using OpenTracing.Propagation;
using OpenTracing.Tag;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.AzureServicebus
{
    internal class AzureServiceBusMessageSender : IMessageQueue, IAsyncDisposable
    {
        private readonly ServiceBusClient _serviceBusClient;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;
        private readonly AzureServiceBusManager _azureServiceBusManager;
        private readonly TracerFactory _tracerFactory;
        private readonly ConcurrentDictionary<string, Task<ServiceBusSender>> _sendersByTopic;
        private readonly ILogger<AzureServiceBusMessageSender> _logger;

        private ITracer Tracer => _tracerFactory.Tracer;

        public AzureServiceBusMessageSender(
            TracerFactory tracerFactory,
            AzureServiceBusNamingConvention namingConvention,
            MessageSerializer messageSerializer,
            AzureServiceBusClientHolder serviceBusClientHolder,
            AzureServiceBusManager azureServiceBusManager,
            ILogger<AzureServiceBusMessageSender> logger)
        {
            _sendersByTopic = new ConcurrentDictionary<string, Task<ServiceBusSender>>();
            _tracerFactory = tracerFactory;
            _serviceBusClient = serviceBusClientHolder.Instance;
            _namingConvention = namingConvention;
            _messageSerializer = messageSerializer;
            _azureServiceBusManager = azureServiceBusManager;
            _logger = logger;
        }

        public async Task SendEvent(object eventMessage, TimeSpan? delay = default)
        {
            var sender = await _sendersByTopic.GetOrAdd(_namingConvention.GetTopicName(eventMessage.GetType()), async topickName =>
            {
                await _azureServiceBusManager.UpdateTopic(topickName);
                return _serviceBusClient.CreateSender(topickName);
            });

            await Send(sender, eventMessage, delay);
        }

        public async Task SendEvents(IEnumerable<object> eventMessages)
        {
            var topics = eventMessages.GroupBy(m => _namingConvention.GetTopicName(m.GetType()));
            foreach (var topicMessages in topics)
            {
                var sender = await _sendersByTopic.GetOrAdd(topicMessages.Key, async topickName =>
                {
                    await _azureServiceBusManager.UpdateTopic(topickName);
                    return _serviceBusClient.CreateSender(topickName);
                });

                await SendMultiple(sender, topicMessages);
            }
        }

        public async Task SendCommand(object commandMessage)
        {
            var sender = await _sendersByTopic.GetOrAdd(_namingConvention.GetQueueName(commandMessage.GetType()), async queueName =>
            {
                await _azureServiceBusManager.UpdateQueue(queueName);
                return _serviceBusClient.CreateSender(queueName);
            });

            await Send(sender, commandMessage);
        }

        public async Task SendCommands(IEnumerable<object> commands)
        {
            var queues = commands.GroupBy(c => _namingConvention.GetQueueName(c.GetType()));

            foreach (var queueMessages in queues)
            {
                var sender = await _sendersByTopic.GetOrAdd(queueMessages.Key, async queueName =>
                {
                    await _azureServiceBusManager.UpdateQueue(queueName);
                    return _serviceBusClient.CreateSender(queueName);
                });

                await SendMultiple(sender, queueMessages);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Task.WhenAll(
                _sendersByTopic.Values.Select(async s => await (await s).DisposeAsync().AsTask())
            );
        }

        private async Task Send(ServiceBusSender sender, object msg, TimeSpan? delay = default)
        {
            try
            {
                await sender.SendMessageAsync(CreateMessage(msg, delay));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Send message error: {ex.Message}");

                throw;
            }
        }

        public async Task SendMultiple(ServiceBusSender sender, IEnumerable<object> eventMessages)
        {
            try
            {
                await sender.SendMessagesAsync(eventMessages.Select(m => CreateMessage(m)));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Send message error: {ex.Message}");

                throw;
            }
        }

        private ServiceBusMessage CreateMessage(object eventMessage, TimeSpan? delay = default)
        {
            ISpan? span = null;

            try
            {
                var type = eventMessage.GetType();
                var serializedMessage = _messageSerializer.SerializeToString(eventMessage, type);


                var wrapped = new Message
                {
                    Payload = eventMessage
                };

                if (Tracer != null)
                {
                    span = Tracer
                        .BuildSpan($"Send {type.Name}")
                        .WithTag(Tags.Component, "MessageQueue")
                        .WithTag(Tags.SpanKind, Tags.SpanKindProducer)
                        .WithTag("mq.message_name", type.Name)
                        .WithTag("mq.message", serializedMessage)
                        .Start();

                    Tracer.Inject(span.Context, BuiltinFormats.TextMap, new TextMapInjectAdapter(wrapped.Headers));
                }
                _logger.LogInformation("Sending Message {PayloadType}, {MessageBody}", type, serializedMessage);

                var bytes = _messageSerializer.Serialize(wrapped);

                var message = new ServiceBusMessage(bytes)
                {
                    ApplicationProperties =
                    {
                        // Warning: subscription filters use this property, don't change.
                        ["EsoTechMessageKind"] = _namingConvention.GetSubscriptionFilterValue(type)
                    }
                };
                if (delay.HasValue)
                    message.ScheduledEnqueueTime = DateTimeOffset.UtcNow + delay.Value;

                return message;
            }
            finally
            {
                span?.Finish();
            }
        }
    }
}
