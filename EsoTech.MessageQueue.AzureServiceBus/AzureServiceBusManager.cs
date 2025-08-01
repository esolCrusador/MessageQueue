using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.AzureServicebus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive;
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
        private readonly ILogger _logger;

        private Task<BusSchema> _initialSchema;
        private ServiceBusAdministrationClient Client => _client ??= new ServiceBusAdministrationClient(_azureOptions.AdministrativeConnectionString ?? _azureOptions.ConnectionString);

        public AzureServiceBusManager(IOptions<MessageQueueConfiguration> messageQueueOptions, IOptions<AzureServiceBusConfiguration> azureOptions, AzureServiceBusNamingConvention namingConvention, ILogger<AzureServiceBusManager> logger)
        {
            _messageQueueOptions = messageQueueOptions.Value;
            _azureOptions = azureOptions.Value;
            _namingConvention = namingConvention;
            _logger = logger;
            _initialSchema = GetSchema(default);
        }

        private object _resetLock = new object();
        private Task? _resetTask;
        public async Task Reset()
        {
            if (_resetTask == null)
                lock (_resetLock)
                    _resetTask ??= Reinitialize();

            await _resetTask;
            _resetTask = null;
        }

        private async Task Reinitialize()
        {
            var initialSchema = await _initialSchema;
            _initialSchema = GetSchema(default);

            foreach (var (topic, subscriptions) in initialSchema.Topics)
            {
                await UpdateTopic(topic);
                foreach (var (subscription, rules) in subscriptions)
                    await UpdateSubscription(topic, subscription, rules);
            }

            foreach (var queue in initialSchema.Queues.Keys)
                await UpdateQueue(queue);
        }

        public Task UpdateSubscription(string topicName, string subscriptionName, IEnumerable<Type> messageTypes)
        {
            return UpdateSubscription(topicName, subscriptionName, GetRuleProperties(messageTypes));
        }

        private async Task UpdateSubscription(string topicName, string subscriptionName, Dictionary<string, RuleParameters> newRules)
        {
            await UpdateTopic(topicName);

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            Dictionary<string, RuleParameters>? existingRules = null;
            var schema = await _initialSchema;
            if (!schema.Topics.TryGetValue(topicName, out var subscriptions) || !subscriptions.TryGetValue(subscriptionName, out existingRules))
            {
                _logger.LogInformation($"Updating subscription {subscriptionName}");
                try
                {
                    await Client.CreateSubscriptionAsync(new CreateSubscriptionOptions(topicName, subscriptionName)
                    {
                        MaxDeliveryCount = _azureOptions.MaxDeliveryCount,
                        LockDuration = _messageQueueOptions.AckTimeout,
                        DefaultMessageTimeToLive = _azureOptions.DefaultMessageTimeToLive,
                        DeadLetteringOnMessageExpiration = true
                    });

                    subscriptions = schema.Topics.GetOrAdd(topicName, _ => new ConcurrentDictionary<string, Dictionary<string, RuleParameters>>());
                    subscriptions.TryAdd(subscriptionName, existingRules = new Dictionary<string, RuleParameters>());
                }
                catch (ServiceBusException ex) when (ex.IsDuplicated())
                {
                    _logger.LogInformation("Already exists {TopicName}/{SubscriptionName}", topicName, subscriptionName);
                    existingRules = (await GetAll(Client.GetRulesAsync(topicName, subscriptionName))).Select(r => new RuleParameters(r)).ToDictionary(r => r.Name);
                }
                _logger.LogInformation($"Subscription {subscriptionName} updated for {stopwatch.ElapsedMilliseconds}ms");
            }
            await UpdateRules(topicName, subscriptionName, newRules, existingRules);

            stopwatch.Stop();
        }

        private Dictionary<string, RuleParameters> GetRuleProperties(IEnumerable<Type> messageTypes)
        {
            return messageTypes.Distinct().Select(mt =>
            {
                var ruleName = _namingConvention.GetSubscriptionFilterName(mt, 50);

                return new RuleParameters()
                {
                    Name = ruleName,
                    Filter = new CorrelationRuleFilter
                    {
                        ApplicationProperties = { ["EsoTechMessageKind"] = _namingConvention.GetSubscriptionFilterValue(mt) }
                    }
                };
            }).ToDictionary(mt => mt.Name);
        }

        private async Task UpdateRules(string topicName, string subscriptionName, Dictionary<string, RuleParameters> rules, Dictionary<string, RuleParameters> existingRules)
        {
            var rulesToDelete = existingRules.Keys.Where(r => !rules.ContainsKey(r)).ToList();
            foreach (var rule in rulesToDelete)
            {
                await Client.DeleteRuleAsync(topicName, subscriptionName, rule);
                existingRules.Remove(rule);
            }
            var rulesToAdd = rules.Where(kvp => !existingRules.ContainsKey(kvp.Key));
            foreach (var (ruleName, ruleParameters) in rulesToAdd)
            {
                var response = await Client.CreateRuleAsync(topicName, subscriptionName, new CreateRuleOptions
                {
                    Name = ruleParameters.Name,
                    Filter = ruleParameters.Filter
                });
                existingRules.Add(ruleName, ruleParameters);
            }
        }

        public async Task UpdateTopic(string topicName)
        {
            var schema = await _initialSchema;
            if (!schema.Topics.ContainsKey(topicName))
            {
                var stopwatch = Stopwatch.StartNew();
                _logger.LogInformation($"Updating topic {topicName}");
                try
                {
                    await Client.CreateTopicAsync(new CreateTopicOptions(topicName)
                    {
                        MaxSizeInMegabytes = _azureOptions.MaxSizeInMB,
                        DefaultMessageTimeToLive = _azureOptions.DefaultMessageTimeToLive,
                    });

                    schema.Topics.TryAdd(topicName, new ConcurrentDictionary<string, Dictionary<string, RuleParameters>>());
                }
                catch (ServiceBusException ex) when (ex.IsDuplicated())
                {
                    _logger.LogInformation("Duplicated {TopicName}", topicName);
                }

                _logger.LogInformation($"Topic {topicName} updated for {stopwatch.ElapsedMilliseconds}ms");
            }
        }

        public async Task UpdateQueue(string queueName)
        {
            var schema = await _initialSchema;
            if (!schema.Queues.ContainsKey(queueName))
            {
                _logger.LogInformation($"Updating queue {queueName}");
                var stopwatch = Stopwatch.StartNew();

                await Client.CreateQueueAsync(new CreateQueueOptions(queueName)
                {
                    MaxSizeInMegabytes = _azureOptions.MaxSizeInMB,
                    LockDuration = _messageQueueOptions.AckTimeout,
                    DefaultMessageTimeToLive = _azureOptions.DefaultMessageTimeToLive
                });
                schema.Queues.TryAdd(queueName, Unit.Default);

                _logger.LogInformation($"Queue {queueName} updated for {stopwatch.ElapsedMilliseconds}ms");
            }
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

            (await _initialSchema).Clear();
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
                schema.Topics.TryAdd(topic.Name, new ConcurrentDictionary<string, Dictionary<string, RuleParameters>>());
                initializations.Add(LoadSubscriptions(schema, topic, cancellationToken));
            }

            await Task.WhenAll(initializations);
        }

        private async Task LoadQueues(BusSchema schema, CancellationToken cancellationToken)
        {
            await foreach (var queue in Client.GetQueuesAsync(cancellationToken))
                schema.Queues.TryAdd(queue.Name, Unit.Default);
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
            schema.Topics[subscription.TopicName].TryAdd(subscription.SubscriptionName, rules.ToDictionary(r => r.Name, r => new RuleParameters(r)));
        }
    }

    internal class BusSchema
    {
        public ConcurrentDictionary<string, ConcurrentDictionary<string, Dictionary<string, RuleParameters>>> Topics { get; } =
            new ConcurrentDictionary<string, ConcurrentDictionary<string, Dictionary<string, RuleParameters>>>();
        public ConcurrentDictionary<string, Unit> Queues { get; } = new ConcurrentDictionary<string, Unit>();

        public void Clear()
        {
            Topics.Clear();
            Queues.Clear();
        }
    }

    struct RuleParameters
    {
        public string Name { get; set; }
        public RuleFilter Filter { get; set; }

        public RuleParameters(RuleProperties ruleProperties)
        {
            Name = ruleProperties.Name;
            Filter = ruleProperties.Filter;
        }
    }
}
