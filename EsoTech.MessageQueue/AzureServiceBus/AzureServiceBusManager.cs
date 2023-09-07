using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class AzureServiceBusManager
    {
        private ServiceBusAdministrationClient? _client;
        private readonly AzureServiceBusConfiguration _configuration;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly ILogger _logger;

        private ServiceBusAdministrationClient Client => _client ??= new ServiceBusAdministrationClient(_configuration.ConnectionString);

        public AzureServiceBusManager(AzureServiceBusConfiguration configuration, AzureServiceBusNamingConvention namingConvention, ILogger<AzureServiceBusManager> logger)
        {
            _configuration = configuration;
            _namingConvention = namingConvention;
            _logger = logger;
        }

        public async Task UpdateSubscription(string topicName, string subscriptionName, IEnumerable<Type> messageTypes)
        {
            await UpdateTopic(topicName);

            _logger.LogInformation($"Updating subscription {subscriptionName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            if (!await Client.SubscriptionExistsAsync(topicName, subscriptionName))
                await Client.CreateSubscriptionAsync(new CreateSubscriptionOptions(topicName, subscriptionName)
                {
                    MaxDeliveryCount = _configuration.MaxDeliveryCount,
                    DefaultMessageTimeToLive = _configuration.DefaultMessageTimeToLive,
                    DeadLetteringOnMessageExpiration = true
                });
            await UpdateRules(topicName, subscriptionName, messageTypes);

            stopwatch.Stop();
            _logger.LogInformation($"Subscription {subscriptionName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        private async Task UpdateRules(string topicName, string subscriptionName, IEnumerable<Type> messageTypes)
        {
            var messageRules = messageTypes.Distinct().ToDictionary(
                mt => _namingConvention.GetSubscriptionFilterName(mt, 50),
                mt => _namingConvention.GetSubscriptionFilterValue(mt)
            );

            var rules = await GetAllRules(topicName, subscriptionName);
            var rulesToDelete = rules.Where(r => !messageRules.ContainsKey(r.Name));
            foreach (var rule in rulesToDelete)
                await Client.DeleteRuleAsync(topicName, subscriptionName, rule.Name);
            var messageTypesToAdd = messageRules.Where(kvp => !rules.Any(r => r.Name == kvp.Key));
            foreach (var (ruleName, messageType) in messageTypesToAdd)
                await Client.CreateRuleAsync(topicName, subscriptionName, new CreateRuleOptions
                {
                    Name = ruleName,
                    Filter = new CorrelationRuleFilter
                    {
                        ApplicationProperties = { ["EsoTechMessageKind"] = messageType }
                    }
                });
        }

        public async Task UpdateTopic(string topicName)
        {
            _logger.LogInformation($"Updating topic {topicName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            if (!await Client.TopicExistsAsync(topicName))
                await Client.CreateTopicAsync(new CreateTopicOptions(topicName)
                {
                    MaxSizeInMegabytes = _configuration.MaxSizeInMB,
                    DefaultMessageTimeToLive = _configuration.DefaultMessageTimeToLive,
                });

            stopwatch.Stop();
            _logger.LogInformation($"Topic {topicName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        public async Task UpdateQueue(string queueName)
        {
            _logger.LogInformation($"Updating queue {queueName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            if (!await Client.QueueExistsAsync(queueName))
                await Client.CreateQueueAsync(new CreateQueueOptions(queueName)
                {
                    MaxSizeInMegabytes = _configuration.MaxSizeInMB,
                    DefaultMessageTimeToLive = _configuration.DefaultMessageTimeToLive
                });

            stopwatch.Stop();
            _logger.LogInformation($"Queue {queueName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        private Task<IList<RuleProperties>> GetAllRules(string topicName, string subscriptionName) =>
            GetAll(Client.GetRulesAsync(topicName, subscriptionName));

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
}
