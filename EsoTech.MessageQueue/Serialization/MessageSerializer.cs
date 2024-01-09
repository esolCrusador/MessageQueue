using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Text.Json;

namespace EsoTech.MessageQueue.Serialization
{
    public class MessageSerializer
    {
        private readonly ILogger<MessageSerializer> _logger;

        private static readonly Histogram DeliveryTime =
            Metrics.CreateHistogram("message_queue_delivery_time_milliseconds", "Time in flight for message queue messages.");

        private readonly JsonSerializerOptions _jsonOptions;

        public MessageSerializer(ILogger<MessageSerializer> logger): this(logger, new JsonSerializerOptions { Converters = { new MessageConverter() }})
        {
        }

        public MessageSerializer(ILogger<MessageSerializer> logger, JsonSerializerOptions jsonSerializerOptions)
        {
            _logger = logger;
            _jsonOptions = jsonSerializerOptions;
        }

        public byte[] Serialize(Message msg)
        {
            msg.TimestampInTicks = DateTime.UtcNow.Ticks;

            return JsonSerializer.SerializeToUtf8Bytes(msg, _jsonOptions);
        }

        public Message Deserialize(ReadOnlySpan<byte> bytes)
        {
            var deserialized = JsonSerializer.Deserialize<Message>(bytes, _jsonOptions);

            if (deserialized?.TimestampInTicks != null)
            {
                var timeToDeliver = DateTime.UtcNow.Ticks - deserialized.TimestampInTicks;
                var timeSpan = TimeSpan.FromTicks(Math.Max(timeToDeliver, 0));

                DeliveryTime.Observe(timeSpan.TotalMilliseconds);
            }

            return deserialized ?? throw new ArgumentException("Could not deserialize", nameof(bytes));
        }

        public bool TryDeserialize(ReadOnlySpan<byte> bytes, out Message? msg)
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
