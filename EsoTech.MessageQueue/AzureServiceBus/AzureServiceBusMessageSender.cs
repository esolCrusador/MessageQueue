using Azure.Messaging.ServiceBus;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OpenTracing;
using OpenTracing.Propagation;
using OpenTracing.Tag;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal class AzureServiceBusMessageSender : IMessageQueue, IAsyncDisposable
    {
        private readonly ServiceBusClient _serviceBusClient;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;
        private readonly MessageQueueConfiguration _messageQueueConfiguration;
        private readonly AzureServiceBusManager _azureServiceBusManager;
        private readonly TracerFactory _tracerFactory;
        private readonly ConcurrentDictionary<string, Task<ServiceBusSender>> _sendersByTopic;
        private readonly ILogger<AzureServiceBusMessageSender> _logger;

        private ITracer Tracer => _tracerFactory.Tracer;

        public AzureServiceBusMessageSender(
            TracerFactory tracerFactory,
            AzureServiceBusNamingConvention namingConvention,
            MessageSerializer messageSerializer,
            MessageQueueConfiguration messageQueueConfiguration,
            AzureServiceBusClientHolder serviceBusClientHolder,
            AzureServiceBusManager azureServiceBusManager,
            ILogger<AzureServiceBusMessageSender> logger)
        {
            _sendersByTopic = new ConcurrentDictionary<string, Task<ServiceBusSender>>();
            _tracerFactory = tracerFactory;
            _serviceBusClient = serviceBusClientHolder.Instance;
            _namingConvention = namingConvention;
            _messageSerializer = messageSerializer;
            _messageQueueConfiguration = messageQueueConfiguration;
            _azureServiceBusManager = azureServiceBusManager;
            _logger = logger;
        }

        public async Task SendEvent(object eventMessage)
        {
            var sender = await _sendersByTopic.GetOrAdd(_namingConvention.GetTopicName(eventMessage.GetType()), async topickName =>
            {
                await _azureServiceBusManager.UpdateTopic(topickName);
                return _serviceBusClient.CreateSender(topickName);
            });

            await Send(sender, eventMessage);
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

                await Send(sender, topicMessages);
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

        public async ValueTask DisposeAsync()
        {
            await Task.WhenAll(
                _sendersByTopic.Values.Select(async s => await (await s).DisposeAsync().AsTask())
            );
        }

        private async Task Send(ServiceBusSender sender, object msg)
        {
            try
            {
                await sender.SendMessageAsync(CreateMessage(msg));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Send message error: {ex.Message}");

                throw;
            }
        }

        public async Task Send(ServiceBusSender sender, IEnumerable<object> eventMessages)
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

        private ServiceBusMessage CreateMessage(object eventMessage)
        {
            ISpan? span = null;

            try
            {
                var type = eventMessage.GetType();

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
                        .WithTag("mq.message", JsonConvert.SerializeObject(eventMessage))
                        .Start();

                    Tracer.Inject(span.Context, BuiltinFormats.TextMap, new TextMapInjectAdapter(wrapped.Headers));
                }

                var bytes = _messageSerializer.Serialize(wrapped);

                return new ServiceBusMessage(bytes)
                {
                    ApplicationProperties =
                    {
                        // Warning: subscription filters use this property, don't change.
                        ["EsoTechMessageKind"] = _namingConvention.GetSubscriptionFilterValue(eventMessage.GetType())
                    }
                };
            }
            finally
            {
                span?.Finish();
            }
        }
    }
}
