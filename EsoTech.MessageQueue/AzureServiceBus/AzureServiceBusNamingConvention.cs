using System;
using System.Linq;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class AzureServiceBusNamingConvention
    {
        public string GetSubscriptionName(Type messageType, Type handlerType)
        {
            return $"{GetTopicName(messageType)}_{GetServiceName(handlerType)}";
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
            return messageType.ToString();
        }

        private static string GetServiceName(Type type)
        {
            var typeFullName = type?.FullName;

            if (typeFullName == null)
                throw new ArgumentException(nameof(type));

            return typeFullName.Split('.').Skip(1).First().ToLower();
        }
    }
}