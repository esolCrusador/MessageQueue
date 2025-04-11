using System;

namespace EsoTech.MessageQueue.Abstractions
{
    public class MessageQueueConfiguration
    {
        private TimeSpan? _resumeLockPeriod;
        public string? ClientId { get; set; }
        public string? ServiceName { get; set; }
        public TimeSpan AckTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan ResumeLockPeriod { get => _resumeLockPeriod ?? AckTimeout / 2; set => _resumeLockPeriod = value; }
        public bool HandleRealtime { get; set; } = true;
        public int MaxConcurrentMessages { get; set; } = 100;
    }
}
