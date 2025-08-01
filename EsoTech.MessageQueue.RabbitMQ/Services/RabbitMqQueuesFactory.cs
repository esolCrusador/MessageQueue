using System.Collections.Concurrent;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Threading;

namespace EsoTech.MessageQueue.RabbitMQ.Services
{
    public class RabbitMqQueueFactory
    {
        private readonly RabbitMQClient _rabbitMQClient;
        private readonly ConcurrentDictionary<string, Lazy<Task>> _topics = new();
        private readonly ConcurrentDictionary<string, Lazy<Task>> _queues = new();
        private readonly ConcurrentDictionary<string, Lazy<Task<string>>> _deadLetterQueues = new();

        public RabbitMqQueueFactory(RabbitMQClient rabbitMQClient)
        {
            _rabbitMQClient = rabbitMQClient;
        }

        public async Task CreateTopic(string topic, CancellationToken cancellationToken)
        {
            Lazy<Task>? publisherTask = default;
            try
            {
                publisherTask = _topics.GetOrAdd(topic, t => new Lazy<Task>(async () =>
                {
                    await _rabbitMQClient.CreateTopic(await _rabbitMQClient.GetSenderChannel(), topic, cancellationToken);
                }));
                await publisherTask.Value;
            }
            catch
            {
                if (publisherTask != null)
                    _topics.TryRemove(new KeyValuePair<string, Lazy<Task>>(topic, publisherTask));
                throw;
            }
        }

        public async Task CreateQueueue(string queueName, CancellationToken cancellationToken)
        {
            Lazy<Task>? publisherTask = default;
            try
            {
                publisherTask = _queues.GetOrAdd(queueName, t => new Lazy<Task>(async () =>
                {
                    await _rabbitMQClient.CreateQueue(await _rabbitMQClient.GetSenderChannel(), queueName, cancellationToken);
                }));
                await publisherTask.Value;
            }
            catch
            {
                if (publisherTask != null)
                    _queues.TryRemove(new KeyValuePair<string, Lazy<Task>>(queueName, publisherTask));
                throw;
            }
        }

        public async Task<string> CreateDeadLetterQueueue(string queueName, CancellationToken cancellationToken)
        {
            Lazy<Task<string>>? publisherTask = default;
            try
            {
                publisherTask = _deadLetterQueues.GetOrAdd(queueName, t => new Lazy<Task<string>>(async () =>
                {
                    return await _rabbitMQClient.CreateDeadletterQueue(await _rabbitMQClient.GetSenderChannel(), queueName, cancellationToken);
                }));
                return await publisherTask.Value;
            }
            catch
            {
                if (publisherTask != null)
                    _deadLetterQueues.TryRemove(new KeyValuePair<string, Lazy<Task<string>>>(queueName, publisherTask));
                throw;
            }
        }
    }
}
