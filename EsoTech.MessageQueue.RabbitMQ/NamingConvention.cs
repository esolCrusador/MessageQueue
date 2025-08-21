using EsoTech.MessageQueue.AzureServiceBus;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace EsoTech.MessageQueue.RabbitMQ
{
    public class NamingConvention
    {
        private readonly Regex _notNeeded = new Regex("(\\w+\\.)|\\[|\\]", RegexOptions.Compiled);
        private readonly Regex _toReplace = new Regex("(`\\d+)|,", RegexOptions.Compiled);
        private readonly HashFunction _hashFunction;
        private readonly Dictionary<string, string> _serviceNamesRemap;
        
        public const string DeadletterQueuePostfix = "-deadletter";
        public const string DeadletterRoutingKeyHeader = "x-deadletter-routing-key";
        public const string RediliveryCountHeader = "x-redelivery-count";

        public NamingConvention(HashFunction hashFunction, IOptions<RabbitMQConfiguration> configuration)
        {
            _hashFunction = hashFunction;
            _serviceNamesRemap = configuration.Value.ServicesRemap;
        }

        public string GetSubscriptionName(Type handlerType)
        {
            return GetServiceName(handlerType);
        }

        public string GetTopicName(Type messageType)
        {
            return $"{GetServiceName(messageType)}";
        }

        public string GetQueueName(Type messageType)
        {
            return $"{GetServiceName(messageType)}commands";
        }

        public string GetTopicSubscriptionName(string topicName, string subscriptionName) => $"{topicName}-{subscriptionName}";

        private const int MaxRoutingKeyLength = 255;

        public string GetRoutingKey(Type messageType)
        {
            var typeName = GetTypeNamePrefix(messageType, int.MaxValue);
            if (typeName.Length <= MaxRoutingKeyLength)
                return typeName;

            var hash = _hashFunction.GetHash(messageType.FullName!);
            return $"{Truncate(typeName, MaxRoutingKeyLength - hash.Length - 1)}-{hash}";
        }

        private string GetServiceName(Type type)
        {
            var assemblyFullName = type.Assembly.FullName;
            assemblyFullName = assemblyFullName!.Substring(0, assemblyFullName.IndexOf(", "));
            var parts = assemblyFullName.Split('.');

            var serviceName = parts.Length > 1 ? parts.Skip(1).First().ToLower() : parts[0];

            return _serviceNamesRemap.GetValueOrDefault(serviceName) ?? serviceName;
        }


        private string GetTypeNamePrefix(Type mt, int maxLength)
        {
            string prefix = Truncate(mt.Name, maxLength);
            if (prefix.Contains('`'))
            {
                // For example GG`2[System.String,System.Collections.Generic.List`1[System.String]] 
                prefix = mt.ToString();
                // Removing "[", "]", namespaces
                prefix = _notNeeded.Replace(prefix, string.Empty);
                // Replacing "`2", "," with "-"
                prefix = _toReplace.Replace(prefix, "-");
                prefix = Truncate(prefix, maxLength);
            }

            return prefix;
        }

        private string Truncate(string str, int maxLength) =>
            str.Length <= maxLength ? str : str.Substring(0, maxLength);
    }
}
