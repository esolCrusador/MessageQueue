using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class AzureServiceBusNamingConvention
    {
        private readonly Regex _notNeeded = new Regex("(\\w+\\.)|\\[|\\]", RegexOptions.Compiled);
        private readonly Regex _toReplace = new Regex("(`\\d+)|,", RegexOptions.Compiled);
        private readonly HashFunction _hashFunction;
        private readonly Dictionary<string, string> _serviceNamesRemap;

        public AzureServiceBusNamingConvention(HashFunction hashFunction, IOptions<AzureServiceBusConfiguration> configuration)
        {
            _hashFunction = hashFunction;
            _serviceNamesRemap = configuration.Value.ServicesRemap;
        }

        public string GetSubscriptionName(Type messageType, Type handlerType)
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

        public string GetSubscriptionFilterValue(Type messageType)
        {
            return _hashFunction.GetHash(messageType.FullName);
        }

        public string GetSubscriptionFilterName(Type messageType, int maxLength)
        {
            string prefix = GetTypeNamePrefix(messageType, maxLength);

            return prefix;
        }

        private string GetServiceName(Type type)
        {
            var assemblyFullName = type.Assembly.FullName;
            assemblyFullName = assemblyFullName.Substring(0, assemblyFullName.IndexOf(", "));

            var serviceName = assemblyFullName.Split('.').Skip(1).First().ToLower();

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