using System.Collections.Generic;

namespace EsoTech.MessageQueue
{
    internal class Message
    {
        public Dictionary<string, string> Headers { get; set; } = new Dictionary<string, string>();

        // TODO: Remove this after measuring.
        public long? TimestampInTicks { get; set; }

        public object Payload { get; set; }
    }
}
