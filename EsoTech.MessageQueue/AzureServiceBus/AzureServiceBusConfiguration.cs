using System;
using System.Collections.Generic;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    public class AzureServiceBusConfiguration
    {
        public string? ConnectionStringName { get; set; }
        public string? ConnectionString { get; set; }
        public int MaxDeliveryCount { get; set; } = 20;
        public long MaxSizeInMB { get; internal set; } = 5 * 1024;
        public TimeSpan DefaultMessageTimeToLive { get; internal set; } = TimeSpan.FromDays(14);
        public Dictionary<string, string> ServicesRemap { get; set; } = new Dictionary<string, string>();
    }
}
