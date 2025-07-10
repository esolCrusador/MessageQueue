using System;
using System.Collections.Generic;

namespace EsoTech.MessageQueue.RabbitMQ
{
    public class RabbitMQConfiguration
    {
        public RabbitMQConnectionConfiguration Connection { get; set; } = new RabbitMQConnectionConfiguration();
        public Dictionary<string, string> ServicesRemap { get; set; } = new Dictionary<string, string>();
        public int MaxConnectionRetries { get; set; } = int.MaxValue;
        public TimeSpan ReconnectTimeout { get; set; } = TimeSpan.FromSeconds(5);
        public int SendingParallelism { get; set; } = 50;
        public int MaxDeliveryCount { get; set; } = 20;
    }
    public class RabbitMQConnectionConfiguration
    {
        public string? Host { get; set; }
        public int Port { get; set; } = 5672;
        public int ManagementPort { get; set; } = 15672;
        public string? User { get; set; }
        public string? Password { get; set; }
        public string VirtualHost { get; set; } = "/";
        public bool UseSsl { get; set; }
        public bool IgnoreSslErrors { get; set; }
    }
}
