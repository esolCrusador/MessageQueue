using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.RabbitMQ.Serialization;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.RabbitMQ.Services
{
    public class RabbitMqMessageQueue : IMessageQueue
    {
        private readonly RabbitMqQueueFactory _rabbitMqQueueFactory;
        private readonly RabbitMQClient _rabbitMQClient;
        private readonly RabbitMqManagement _rabbitMqManager;
        private readonly RabbitMQConfiguration _options;
        private readonly NamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;

        private readonly ConcurrentDictionary<Type, string> _topicNames = new();
        private readonly ConcurrentDictionary<Type, string> _routingKeys = new();
        private readonly ConcurrentDictionary<Type, string> _queueNames = new();

        public RabbitMqMessageQueue(RabbitMqQueueFactory rabbitMqQueueFactory, RabbitMQClient rabbitMQClient, RabbitMqManagement rabbitMqManager, IOptions<RabbitMQConfiguration> options, NamingConvention namingConvention, MessageSerializer messageSerializer)
        {
            _rabbitMqQueueFactory = rabbitMqQueueFactory;
            _rabbitMQClient = rabbitMQClient;
            _rabbitMqManager = rabbitMqManager;
            _options = options.Value;
            _namingConvention = namingConvention;
            _messageSerializer = messageSerializer;
        }

        public async Task SendEvent(object eventMessage, TimeSpan? delay = null)
        {
            if (delay.HasValue)
                throw new NotSupportedException($"Delayed publishing is not supported");

            var messageType = eventMessage.GetType();
            var topicName = _topicNames.GetOrAdd(messageType, _namingConvention.GetTopicName);
            var routingKey = _routingKeys.GetOrAdd(messageType, _namingConvention.GetRoutingKey);

            await _rabbitMqQueueFactory.CreateTopic(topicName, default);

            await PublishMessage(topicName, eventMessage, default);
        }

        public Task SendEvents(IEnumerable<object> eventMessages)
        {
            return Parallel.ForEachAsync(eventMessages,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = _options.SendingParallelism
                },
                (message, _) => new ValueTask(SendEvent(message))
            );
        }

        public async Task PublishCommand(object message, CancellationToken cancellationToken)
        {
            var messageType = message.GetType();
            var queueName = _queueNames.GetOrAdd(messageType, _namingConvention.GetQueueName);

            await _rabbitMqQueueFactory.CreateQueueue(queueName, cancellationToken);

            await PublishMessage(queueName, message, cancellationToken);
        }

        public async Task SendCommand(object commandMessage)
        {
            var messageType = commandMessage.GetType();
            var queueName = _queueNames.GetOrAdd(messageType, _namingConvention.GetQueueName);

            await _rabbitMqQueueFactory.CreateQueueue(queueName, default);

            await PublishMessage(queueName, commandMessage, default);
        }

        public Task SendCommands(IEnumerable<object> commands)
        {
            return Parallel.ForEachAsync(commands,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = _options.SendingParallelism
                },
                (message, _) => new ValueTask(SendCommand(message))
            );
        }

        private async Task PublishMessage(string destanation, object message, CancellationToken cancellationToken)
        {
            var messageType = message.GetType();
            var routingKey = _routingKeys.GetOrAdd(messageType, _namingConvention.GetRoutingKey);

            await using var channelLock = await _rabbitMQClient.CaptureSenderChannel();
            var channel = channelLock.Channel;

            await channel.BasicPublishAsync(destanation, routingKey, true,
            new BasicProperties
            {
                CorrelationId = Guid.NewGuid().ToString(),
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    [NamingConvention.RediliveryCountHeader] = "0"
                }
            },
            _messageSerializer.Serialize(new Message
            {
                MessageId = Guid.NewGuid().ToString("n"),
                PayloadTypeName = messageType.AssemblyQualifiedName,
                Payload = message,
                TimestampInTicks = DateTimeOffset.UtcNow.Ticks
            }),
            cancellationToken);
        }

        public Task<int> RepublishErrorQueues(string? filter, CancellationToken cancellationToken) =>
            HandleErrorQueues(filter, true, cancellationToken);


        public Task<int> CleanupErrorQueues(string? filter, CancellationToken cancellationToken) =>
            HandleErrorQueues(filter, false, cancellationToken);

        private async Task<int> HandleErrorQueues(string? filter, bool republish, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(filter))
                filter = Regex.Escape(NamingConvention.DeadletterQueuePostfix);
            else if (!filter.Contains(NamingConvention.DeadletterQueuePostfix))
                filter = $"{Regex.Escape(filter)}.*{Regex.Escape(NamingConvention.DeadletterQueuePostfix)}$";
            else
                filter = Regex.Escape(filter);

            int count = 0;
            ChannelsPool.ChannelLock? channelLock = default;

            try
            {
                var queues = await _rabbitMqManager.GetQueueStats(filter, cancellationToken);
                foreach (var queue in queues)
                {
                    channelLock ??= await _rabbitMQClient.CaptureSenderChannel();
                    var channel = channelLock.Channel;

                    BasicGetResult? result;
                    while ((result = await channel.BasicGetAsync(queue.Name, false, cancellationToken)) != null)
                    {
                        if (republish)
                        {
                            var originalRoutingKey = result.BasicProperties.Headers?
                                    .TryGetValue(NamingConvention.DeadletterRoutingKeyHeader, out var rawHeaderValue) == true
                                    && rawHeaderValue is byte[] rawKey
                                        ? Encoding.UTF8.GetString(rawKey)
                                        : "";

                            var properties = new BasicProperties(result.BasicProperties)
                            {
                                Headers = new Dictionary<string, object?>(result.BasicProperties.Headers ?? new Dictionary<string, object?>()),
                                Persistent = true,
                                DeliveryMode = DeliveryModes.Persistent
                            };

                            properties.Headers.Remove(NamingConvention.DeadletterRoutingKeyHeader);
                            properties.Headers[NamingConvention.RediliveryCountHeader] = "0";

                            await channel.BasicPublishAsync(queue.Name.Substring(0, queue.Name.Length - NamingConvention.DeadletterQueuePostfix.Length), originalRoutingKey, result.Body, cancellationToken);
                        }
                        await channel.BasicAckAsync(result.DeliveryTag, false, cancellationToken);
                        count++;
                    }
                }
            }
            finally
            {
                await (channelLock?.DisposeAsync() ?? default);
            }

            return count;
        }
    }
}
