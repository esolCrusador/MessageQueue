using System;
using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Prometheus;

namespace EsoTech.MessageQueue
{
    internal class MessageSerializer
    {
        private readonly ILogger<MessageSerializer> _logger;

        private static readonly Histogram DeliveryTime =
            Metrics.CreateHistogram("message_queue_delivery_time_milliseconds", "Time in flight for message queue messages.");

        private static readonly JsonSerializerSettings JsonSettings = new JsonSerializerSettings
        {
            TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
            TypeNameHandling = TypeNameHandling.All,
            SerializationBinder = new MessageTypeRenamedSerializationBinder()
        };

        public MessageSerializer(ILogger<MessageSerializer> logger)
        {
            _logger = logger;
        }

        public byte[] Serialize(Message msg)
        {
            msg.TimestampInTicks = DateTime.UtcNow.Ticks;

            var utf8 = JsonConvert.SerializeObject(msg, JsonSettings);
            var bytes = Encoding.UTF8.GetBytes(utf8);

            return bytes;
        }

        public Message Deserialize(ReadOnlySpan<byte> bytes)
        {
            var utf8 = Encoding.UTF8.GetString(bytes);
            var deserialized = JsonConvert.DeserializeObject<Message>(utf8, JsonSettings);

            if (deserialized?.TimestampInTicks != null)
            {
                var timeToDeliver = DateTime.UtcNow.Ticks - deserialized.TimestampInTicks.Value;
                var timeSpan = TimeSpan.FromTicks(Math.Max(timeToDeliver, 0));

                DeliveryTime.Observe(timeSpan.TotalMilliseconds);
            }

            return deserialized;
        }

        public bool TryDeserialize(ReadOnlySpan<byte> bytes, out Message msg)
        {
            try
            {
                msg = Deserialize(bytes);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Could not deserialize message.");

                msg = null;

                return false;
            }
        }

        // TODO: Remove it when migration to azure service bus is done.
        internal class MessageTypeRenamedSerializationBinder : DefaultSerializationBinder
        {
            public override Type BindToType(string assemblyName, string typeName)
            {
                if (typeName == "EsoTech.MessageQueue.Nats.Message")
                    return typeof(Message);

                return base.BindToType(assemblyName, typeName);
            }
        }
    }
}
