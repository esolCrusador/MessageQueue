using System;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace EsoTech.MessageQueue.Serialization
{
    internal class MessageSerializer
    {
        private readonly ILogger<MessageSerializer> _logger;

        private static readonly Histogram DeliveryTime =
            Metrics.CreateHistogram("message_queue_delivery_time_milliseconds", "Time in flight for message queue messages.");

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            Converters = { new MessageConverter() }
        };

        public MessageSerializer(ILogger<MessageSerializer> logger)
        {
            _logger = logger;
        }

        public byte[] Serialize(Message msg)
        {
            msg.TimestampInTicks = DateTime.UtcNow.Ticks;

            return JsonSerializer.SerializeToUtf8Bytes(msg, JsonOptions);
        }

        public Message Deserialize(ReadOnlySpan<byte> bytes)
        {
            var deserialized = JsonSerializer.Deserialize<Message>(bytes, JsonOptions);

            if (deserialized?.TimestampInTicks != null)
            {
                var timeToDeliver = DateTime.UtcNow.Ticks - deserialized.TimestampInTicks;
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
    }
}
