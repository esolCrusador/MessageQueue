using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServiceBus;
using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OpenTracing;
using OpenTracing.Propagation;
using OpenTracing.Tag;

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

                var message = new ServiceBusMessage(bytes)
                {
                    ApplicationProperties =
                    {
                        // Warning: subscription filters use this property, don't change.
                        ["EsoTechMessageKind"] = _namingConvention.GetSubscriptionFilterValue(msg.GetType())
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
