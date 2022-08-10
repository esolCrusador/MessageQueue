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
            return messageType.GUID.ToString("N");
        }

        public string GetSubscriptionFilterName(Type messageType, int maxLength)
        {
            var typeName = this.GetSubscriptionFilterValue(messageType);
            string prefix = GetTypeNamePrefix(messageType, maxLength - 1 - typeName.Length);

            return $"{prefix}-{typeName}";
        }

        private static string GetServiceName(Type type)
        {
            var typeFullName = type?.FullName;

            if (typeFullName == null)
                throw new ArgumentException(nameof(type));

            return typeFullName.Split('.').Skip(1).First().ToLower();
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
    }
}