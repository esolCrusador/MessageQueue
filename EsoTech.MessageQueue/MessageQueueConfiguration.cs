using EsoTech.MessageQueue.AzureServiceBus;

namespace EsoTech.MessageQueue
{
    internal class MessageQueueConfiguration
    {
        public string ClientId { get; }
        public string? ServiceName { get; }
        public int AckTimeoutMilliseconds { get; }
        public bool HandleRealtime { get; }
        public int MaxRedeliveryCount { get; }
        public int MaxConcurrentMessages { get; }
        public AzureServiceBusConfiguration AzureServiceBusConfiguration { get; }

        public MessageQueueConfiguration(string clientId,
            string? serviceName,
            int ackTimeoutMilliseconds,
            bool handleRealtime,
            int maxRedeliveryCount,
            int maxConcurrentMessages,
            AzureServiceBusConfiguration azureServiceBusConfiguration)
        {
            ClientId = clientId;
            ServiceName = serviceName;
            AckTimeoutMilliseconds = ackTimeoutMilliseconds;
            HandleRealtime = handleRealtime;
            MaxRedeliveryCount = maxRedeliveryCount;
            MaxConcurrentMessages = maxConcurrentMessages;
            AzureServiceBusConfiguration = azureServiceBusConfiguration;
        }
    }
}
