using Azure.Messaging.ServiceBus.Administration;
using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class AzureServiceBusManager
    {
        private ServiceBusAdministrationClient? _client;
        private readonly MessageQueueConfiguration _messageQueueOptions;
        private readonly AzureServiceBusConfiguration _azureOptions;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly Task<BusSchema> _initialSchema;
        private readonly ILogger _logger;

        private ServiceBusAdministrationClient Client => _client ??= new ServiceBusAdministrationClient(_azureOptions.ConnectionString);


        public AzureServiceBusManager(IOptions<MessageQueueConfiguration> messageQueueOptions, IOptions<AzureServiceBusConfiguration> azureOptions, AzureServiceBusNamingConvention namingConvention, ILogger<AzureServiceBusManager> logger)
        {
            _messageQueueOptions = messageQueueOptions.Value;
            _azureOptions = azureOptions.Value;
            _namingConvention = namingConvention;
            _logger = logger;
            _initialSchema = GetSchema(default);
        }

        private async Task<BusSchema> GetSchema(CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();
            _logger.LogInformation("Retriving schema");
            var schema = new BusSchema();

            await Task.WhenAll(LoadTopics(schema, cancellationToken), LoadQueues(schema, cancellationToken));

            _logger.LogInformation("Schema loaded. {Milliseconds} milliseconds", sw.ElapsedMilliseconds);
            return schema;
        }

        private async Task LoadTopics(BusSchema schema, CancellationToken cancellationToken)
        {
            List<Task> initializations = new List<Task>();
            await foreach (var topic in Client.GetTopicsAsync(cancellationToken))
            {
                schema.TryAdd(topic.Name, new ConcurrentDictionary<string, IList<RuleProperties>>());
                initializations.Add(LoadSubscriptions(schema, topic, cancellationToken));
            }

            await Task.WhenAll(initializations);
        }

        private async Task LoadQueues(BusSchema schema, CancellationToken cancellationToken)
        {
            await foreach (var queue in Client.GetQueuesAsync(cancellationToken))
                schema.TryAdd(queue.Name, new ConcurrentDictionary<string, IList<RuleProperties>>());
        }

        private async Task LoadSubscriptions(BusSchema schema, TopicProperties topicProperties, CancellationToken cancellationToken)
        {
            List<Task> initializations = new List<Task>();
            await foreach (var subscription in Client.GetSubscriptionsAsync(topicProperties.Name, cancellationToken))
                initializations.Add(LoadRules(schema, subscription, cancellationToken));

            await Task.WhenAll(initializations);
        }

        private async Task LoadRules(BusSchema schema, SubscriptionProperties subscription, CancellationToken cancellationToken)
        {
            IList<RuleProperties> rules = await GetAll(Client.GetRulesAsync(subscription.TopicName, subscription.SubscriptionName, cancellationToken));
            schema[subscription.TopicName].TryAdd(subscription.SubscriptionName, rules);
        }

        public async Task UpdateSubscription(string topicName, string subscriptionName, IEnumerable<Type> messageTypes)
        {
            await UpdateTopic(topicName);

            _logger.LogInformation($"Updating subscription {subscriptionName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            IList<RuleProperties>? rules = null;
            var schema = await _initialSchema;
            if (!schema.TryGetValue(topicName, out var subscriptions) || !subscriptions.TryGetValue(subscriptionName, out rules))
            {
                await Client.CreateSubscriptionAsync(new CreateSubscriptionOptions(topicName, subscriptionName)
                {
                    MaxDeliveryCount = _azureOptions.MaxDeliveryCount,
                    LockDuration = _messageQueueOptions.AckTimeout,
                    DefaultMessageTimeToLive = _azureOptions.DefaultMessageTimeToLive,
                    DeadLetteringOnMessageExpiration = true
                });

                subscriptions = schema.GetOrAdd(topicName, _ => new ConcurrentDictionary<string, IList<RuleProperties>>());
                subscriptions.TryAdd(subscriptionName, rules = new List<RuleProperties>());
            }
            await UpdateRules(topicName, subscriptionName, messageTypes, rules);

            stopwatch.Stop();
            _logger.LogInformation($"Subscription {subscriptionName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        private async Task UpdateRules(string topicName, string subscriptionName, IEnumerable<Type> messageTypes, IList<RuleProperties> rules)
        {
            var messageRules = messageTypes.Distinct().ToDictionary(
                mt => _namingConvention.GetSubscriptionFilterName(mt, 50),
                mt => _namingConvention.GetSubscriptionFilterValue(mt)
            );
            rules ??= new List<RuleProperties>();

            var rulesToDelete = rules.Where(r => !messageRules.ContainsKey(r.Name)).ToList();
            foreach (var rule in rulesToDelete)
            {
                await Client.DeleteRuleAsync(topicName, subscriptionName, rule.Name);
                rules.Remove(rule);
            }
            var messageTypesToAdd = messageRules.Where(kvp => !rules.Any(r => r.Name == kvp.Key));
            foreach (var (ruleName, messageType) in messageTypesToAdd)
            {
                var response = await Client.CreateRuleAsync(topicName, subscriptionName, new CreateRuleOptions
                {
                    Name = ruleName,
                    Filter = new CorrelationRuleFilter
                    {
                        ApplicationProperties = { ["EsoTechMessageKind"] = messageType }
                    }
                });
                rules.Add(response.Value);
            }
        }

        public async Task UpdateTopic(string topicName)
        {
            _logger.LogInformation($"Updating topic {topicName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var schema = await _initialSchema;
            if (!schema.ContainsKey(topicName))
            {
                await Client.CreateTopicAsync(new CreateTopicOptions(topicName)
                {
                    MaxSizeInMegabytes = _azureOptions.MaxSizeInMB,
                    DefaultMessageTimeToLive = _azureOptions.DefaultMessageTimeToLive,
                });

                schema.TryAdd(topicName, new ConcurrentDictionary<string, IList<RuleProperties>>());
            }

            stopwatch.Stop();
            _logger.LogInformation($"Topic {topicName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        public async Task UpdateQueue(string queueName)
        {
            _logger.LogInformation($"Updating queue {queueName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            var schema = await _initialSchema;
            if (!schema.ContainsKey(queueName))
            {
                await Client.CreateQueueAsync(new CreateQueueOptions(queueName)
                {
                    MaxSizeInMegabytes = _azureOptions.MaxSizeInMB,
                    LockDuration = _messageQueueOptions.AckTimeout,
                    DefaultMessageTimeToLive = _azureOptions.DefaultMessageTimeToLive
                });
                schema.TryAdd(queueName, new ConcurrentDictionary<string, IList<RuleProperties>>());
            }

            stopwatch.Stop();
            _logger.LogInformation($"Queue {queueName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        private static async Task<IList<TEntity>> GetAll<TEntity>(IAsyncEnumerable<TEntity> results)
        {
            var entities = new List<TEntity>();
            await foreach (var entry in results)
                entities.Add(entry);

            return entities;
        }

        public async Task PurgeAll()
        {
            var queues = await GetAll(Client.GetQueuesAsync());
            await Task.WhenAll(queues.Select(queue => Client.DeleteQueueAsync(queue.Name)));
            var topics = await GetAll(Client.GetTopicsAsync());
            await Task.WhenAll(topics.Select(topick => Client.DeleteTopicAsync(topick.Name)));
        }
    }

    public class BusSchema : ConcurrentDictionary<string, ConcurrentDictionary<string, IList<RuleProperties>>>
    {
    }
}
