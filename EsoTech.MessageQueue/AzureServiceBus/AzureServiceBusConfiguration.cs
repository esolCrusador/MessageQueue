using System;
using System.Collections.Generic;
using System.Text;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class AzureServiceBusConfiguration
    {
        public string ConnectionString { get; }
        public int MaxDeliveryCount { get; set; } = 20;
        public long MaxSizeInMB { get; internal set; } = 5 * 1024;
        public TimeSpan DefaultMessageTimeToLive { get; internal set; } = TimeSpan.FromDays(14);
        public AzureServiceBusConfiguration(string connectionString) => ConnectionString = connectionString;
    }
}
