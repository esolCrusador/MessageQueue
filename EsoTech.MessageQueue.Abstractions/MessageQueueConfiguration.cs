using System;

namespace EsoTech.MessageQueue.Abstractions
{
    public class MessageQueueConfiguration
    {
        public string? ClientId { get; set; }
        public string? ServiceName { get; set; }
        public TimeSpan AckTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public bool HandleRealtime { get; set; } = true;
        public int MaxConcurrentMessages { get; set; } = 100;
    }
}
