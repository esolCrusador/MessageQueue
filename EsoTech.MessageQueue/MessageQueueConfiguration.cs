namespace EsoTech.MessageQueue
{
    internal class MessageQueueConfiguration
    {
        public string ClientId { get; }
        public string ServiceName { get; }
        public int AckTimeoutMilliseconds { get; }
        public bool HandleRealtime { get; }
        public int MaxRedeliveryCount { get; }
        public int MaxConcurrentMessages { get; }
        public string ConnectionString { get; }

        public MessageQueueConfiguration(string clientId,
            string serviceName,
            int ackTimeoutMilliseconds,
            bool handleRealtime,
            int maxRedeliveryCount,
            int maxConcurrentMessages,
            string connectionString)
        {
            ClientId = clientId;
            ServiceName = serviceName;
            AckTimeoutMilliseconds = ackTimeoutMilliseconds;
            HandleRealtime = handleRealtime;
            MaxRedeliveryCount = maxRedeliveryCount;
            MaxConcurrentMessages = maxConcurrentMessages;
            ConnectionString = connectionString;
        }
    }
}
