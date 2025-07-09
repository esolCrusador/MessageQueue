using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.RabbitMQ
{
    public class RabbitMqMessageQueue : IMessageQueue, IAsyncDisposable
    {
        private readonly RabbitMQClient _rabbitMQClient;
        private readonly RabbitMQConfiguration _options;
        private readonly NamingConvention _namingConvention;
        private readonly MessageSerializer _messageSerializer;

        private SemaphoreSlim _channelLock = new SemaphoreSlim(1);
        private IChannel? _channel;

        private readonly ConcurrentDictionary<string, Lazy<Task>> _topics = new();
        private readonly ConcurrentDictionary<Type, string> _topicNames = new();
        private readonly ConcurrentDictionary<Type, string> _routingKeys = new();
        private readonly ConcurrentDictionary<string, Lazy<Task>> _queues = new();
        private readonly ConcurrentDictionary<Type, string> _queueNames = new();

        public RabbitMqMessageQueue(RabbitMQClient rabbitMQClient, IOptions<RabbitMQConfiguration> options, NamingConvention namingConvention, MessageSerializer messageSerializer)
        {
            _rabbitMQClient = rabbitMQClient;
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

            await CreateTopic(topicName, default);

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

            await CreateQueueue(queueName, cancellationToken);

            await PublishMessage(queueName, message, cancellationToken);
        }

        public async Task SendCommand(object commandMessage)
        {
            var messageType = commandMessage.GetType();
            var queueName = _queueNames.GetOrAdd(messageType, _namingConvention.GetQueueName);

            await CreateQueueue(queueName, default);

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
            var channel = await GetChannel();

            await channel.BasicPublishAsync(destanation, routingKey, true,
            new BasicProperties
            {
                CorrelationId = Guid.NewGuid().ToString(),
                ContentType = "application/json",
                Headers = new Dictionary<string, object?>
                {
                    ["x-redelivery-count"] = "0"
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

        private async Task CreateTopic(string topic, CancellationToken cancellationToken)
        {
            Lazy<Task>? publisherTask = default;
            try
            {
                publisherTask = _topics.GetOrAdd(topic, t => new Lazy<Task>(async () =>
                {
                    await _rabbitMQClient.CreateTopic(await GetChannel(), topic, cancellationToken);
                }));
                await publisherTask.Value;
            }
            catch
            {
                if (publisherTask != null)
                    _topics.TryRemove(new KeyValuePair<string, Lazy<Task>>(topic, publisherTask));
            }
        }

        private async Task CreateQueueue(string queueName, CancellationToken cancellationToken)
        {
            Lazy<Task>? publisherTask = default;
            try
            {
                publisherTask = _queues.GetOrAdd(queueName, t => new Lazy<Task>(async () =>
                {
                    await _rabbitMQClient.CreateQueue(await GetChannel(), queueName, cancellationToken);
                }));
                await publisherTask.Value;
            }
            catch
            {
                if (publisherTask != null)
                    _topics.TryRemove(new KeyValuePair<string, Lazy<Task>>(queueName, publisherTask));
            }
        }

        private async Task<IChannel> GetChannel()
        {
            if (_channel != null && _channel.IsOpen)
                return _channel;

            await _channelLock.WaitAsync();

            try
            {
                if (_channel != null && _channel.IsOpen)
                    return _channel;

                return _channel = await _rabbitMQClient.CreateChannel();
            }
            finally
            {
                _channelLock.Release();
            }
        }

        public ValueTask DisposeAsync()
        {
            _channelLock.Dispose();
            return _channel?.DisposeAsync() ?? default;
        }
    }
}
