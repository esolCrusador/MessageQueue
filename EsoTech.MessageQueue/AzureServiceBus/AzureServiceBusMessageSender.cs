using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OpenTracing;
using OpenTracing.Propagation;
using OpenTracing.Tag;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal class AzureServiceBusMessageSender : IMessageQueue, IAsyncDisposable
    {
        private const string MessageKindProperty = "EsoTechMessageKind";

        private readonly ServiceBusClient _serviceBusClient;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;
        private readonly MessageQueueConfiguration _messageQueueConfiguration;
        private readonly TracerFactory _tracerFactory;
        private readonly ConcurrentDictionary<string, ServiceBusSender> _sendersByTopic;
        private readonly ILogger<AzureServiceBusMessageSender> _logger;

        private ITracer Tracer => _tracerFactory.Tracer;

        public AzureServiceBusMessageSender(
            TracerFactory tracerFactory,
            AzureServiceBusNamingConvention namingConvention,
            MessageSerializer messageSerializer,
            MessageQueueConfiguration messageQueueConfiguration,
            AzureServiceBusClientHolder serviceBusClientHolder,
            ILogger<AzureServiceBusMessageSender> logger)
        {
            _sendersByTopic = new ConcurrentDictionary<string, ServiceBusSender>();
            _tracerFactory = tracerFactory;
            _serviceBusClient = serviceBusClientHolder.Instance;
            _namingConvention = namingConvention;
            _messageSerializer = messageSerializer;
            _messageQueueConfiguration = messageQueueConfiguration;
            _logger = logger;
        }

        public Task SendEvent(object eventMessage) => Send(eventMessage, _namingConvention.GetTopicName(eventMessage.GetType()));

        public Task SendCommand(object commandMessage) => Send(commandMessage, _namingConvention.GetQueueName(commandMessage.GetType()));

        public async ValueTask DisposeAsync()
        {
            await Task.WhenAll(
                _sendersByTopic.Values.Select(s => s.DisposeAsync().AsTask())
            );
        }

        private async Task Send(object msg, string queueName)
        {
            ISpan span = null;

            try
            {
                var type = msg.GetType();

                var wrapped = new Message
                {
                    Payload = msg
                };

                if (Tracer != null)
                {
                    span = Tracer
                        .BuildSpan($"Send {type.Name}")
                        .WithTag(Tags.Component, "MessageQueue")
                        .WithTag(Tags.SpanKind, Tags.SpanKindProducer)
                        .WithTag("mq.message_name", type.Name)
                        .WithTag("mq.message", JsonConvert.SerializeObject(msg))
                        .Start();

                    Tracer.Inject(span.Context, BuiltinFormats.TextMap,
                        new TextMapInjectAdapter(wrapped.Headers));
                }

                var bytes = _messageSerializer.Serialize(wrapped);
                var sender = _sendersByTopic.GetOrAdd(queueName, t => _serviceBusClient.CreateSender(t));

                var message = new ServiceBusMessage(bytes)
                {
                    ApplicationProperties =
                    {
                        // Warning: subscription filters use this property, don't change.
                        { MessageKindProperty, _namingConvention.GetSubscriptionFilterValue(msg.GetType()) }
                    }
                };

                await sender.SendMessageAsync(message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Send message error: {ex.Message}");
                
                throw;
            }
            finally
            {
                span?.Finish();
            }
        }
    }
}
