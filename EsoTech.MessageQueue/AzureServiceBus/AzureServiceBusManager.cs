using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class AzureServiceBusManager
    {
        private readonly Dictionary<string, HashSet<string>> _topicks = new Dictionary<string, HashSet<string>>();
        private readonly HashSet<string> _queues = new HashSet<string>();

        private ManagementClient _client;
        private readonly AzureServiceBusConfiguration _configuration;
        private readonly AzureServiceBusNamingConvention _namingConvention;
        private readonly ILogger _logger;

        private ManagementClient Client => _client ??= new ManagementClient(_configuration.ConnectionString);

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

            var subscriptions = _topicks[topicName];
            if (!subscriptions.Contains(subscriptionName))
            {
                if (!await Client.SubscriptionExistsAsync(topicName, subscriptionName))
                    await Client.CreateSubscriptionAsync(new SubscriptionDescription(topicName, subscriptionName)
                    {
                        MaxDeliveryCount = _configuration.MaxDeliveryCount,
                        DefaultMessageTimeToLive = _configuration.DefaultMessageTimeToLive
                    });
                var messageRules = messageTypes.Distinct().ToDictionary(mt =>
                {
                    var typeName = _namingConvention.GetSubscriptionFilterValue(mt);
                    string prefix = GetTypeNamePrefix(mt, 50 - 1 - typeName.Length);

                    return $"{prefix}-{typeName}";
                }, mt => _namingConvention.GetSubscriptionFilterValue(mt));

                var rules = await GetAllRules(topicName, subscriptionName);
                var rulesToDelete = rules.Where(r => !messageRules.ContainsKey(r.Name));
                foreach (var rule in rulesToDelete)
                    await Client.DeleteRuleAsync(topicName, subscriptionName, rule.Name);
                var messageTypesToAdd = messageRules.Where(kvp => !rules.Any(r => r.Name == kvp.Key));
                foreach (var (ruleName, messageType) in messageTypesToAdd)
                    await Client.CreateRuleAsync(topicName, subscriptionName, new RuleDescription
                    {
                        Name = ruleName,
                        Filter = new CorrelationFilter
                        {
                            Properties = { ["EsoTechMessageKind"] = messageType }
                        }
                    });

                subscriptions.Add(subscriptionName);
            }

            stopwatch.Stop();
            _logger.LogInformation($"Subscription {subscriptionName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        public async Task UpdateTopic(string topicName)
        {
            _logger.LogInformation($"Updating topick {topicName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            if (!_topicks.ContainsKey(topicName))
            {
                if (!await Client.TopicExistsAsync(topicName))
                    await Client.CreateTopicAsync(new TopicDescription(topicName)
                    {
                        MaxSizeInMB = _configuration.MaxSizeInMB,
                        DefaultMessageTimeToLive = _configuration.DefaultMessageTimeToLive
                    });
                _topicks.Add(topicName, new HashSet<string>());
            }

            stopwatch.Stop();
            _logger.LogInformation($"Topick {topicName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        public async Task UpdateQueue(string queueName)
        {
            _logger.LogInformation($"Updating queue {queueName}");
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            if (!_queues.Contains(queueName))
            {
                if (!await Client.QueueExistsAsync(queueName))
                    await Client.CreateQueueAsync(new QueueDescription(queueName)
                    {
                        MaxSizeInMB = _configuration.MaxSizeInMB,
                        DefaultMessageTimeToLive = _configuration.DefaultMessageTimeToLive
                    });
                _queues.Add(queueName);
            }

            stopwatch.Stop();
            _logger.LogInformation($"Queue {queueName} updated for {stopwatch.ElapsedMilliseconds}ms");
        }

        private Task<IList<RuleDescription>> GetAllRules(string topicName, string subscriptionName) =>
            GetAll((take, skip) => Client.GetRulesAsync(topicName, subscriptionName));

        private static async Task<IList<TEntity>> GetAll<TEntity>(Func<int, int, Task<IList<TEntity>>> fetch)
        {
            int skip = 0;
            const int take = 100;

            IList<TEntity> entitites = null;
            do
            {
                var loadedRules = await fetch(take, skip);
                if (entitites == null)
                    entitites = loadedRules;
                else
                    foreach (var r in loadedRules)
                        entitites.Add(r);
            } while (entitites.Count % take == 0);

            return entitites;
        }

        private string GetTypeNamePrefix(Type mt, int maxLength)
        {
            string prefix = Truncate(mt.Name, maxLength);
            Type genericArgumentType = mt;
            int tildaIndex;
            while ((tildaIndex = prefix.LastIndexOf('`')) != -1)
            {
                genericArgumentType = genericArgumentType.GetGenericArguments()[0];
                prefix = Truncate($"{prefix.Substring(0, tildaIndex)}-{genericArgumentType.Name}", maxLength);
            }

            return prefix;
        }

        private string Truncate(string str, int maxLength) =>
            str.Length <= maxLength ? str : str.Substring(0, maxLength);

        public async Task PurgeAll()
        {
            var queues = await GetAll((take, skip) => Client.GetQueuesAsync(take, skip));
            await Task.WhenAll(queues.Select(queue => Client.DeleteQueueAsync(queue.Path)));
            var topics = await GetAll((taks, skip) => Client.GetTopicsAsync(taks, skip));
            await Task.WhenAll(topics.Select(topick => Client.DeleteTopicAsync(topick.Path)));
        }
    }
}
