using System.Collections.Generic;

namespace EsoTech.MessageQueue
{
    public class Message
    {
        public string? PayloadTypeName { get; set; }
        public Dictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();
        public long TimestampInTicks { get; set; }

        public object? Payload { get; set; }
    }
}
