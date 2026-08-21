using Microsoft.Extensions.Logging;
using System;
using System.Text.Json;

namespace EsoTech.MessageQueue.RabbitMQ.Serialization
{
    public class MessageSerializer
    {
        private readonly ILogger<MessageSerializer> _logger;

        private readonly JsonSerializerOptions _jsonOptions;

        public MessageSerializer(ILogger<MessageSerializer> logger) : this(logger, new JsonSerializerOptions())
        {
        }

        public MessageSerializer(ILogger<MessageSerializer> logger, JsonSerializerOptions jsonSerializerOptions)
        {
            _logger = logger;
            _jsonOptions = new JsonSerializerOptions(jsonSerializerOptions)
            {
                Converters = { new MessageConverter() }
            };
        }

        public byte[] Serialize(Message msg)
        {
            msg.TimestampInTicks = DateTime.UtcNow.Ticks;

            return JsonSerializer.SerializeToUtf8Bytes(msg, _jsonOptions);
        }

        public string SerializeToString(object value, Type type) =>
            JsonSerializer.Serialize(value, type, _jsonOptions);

        public Message Deserialize(ReadOnlySpan<byte> bytes)
        {
            var deserialized = JsonSerializer.Deserialize<Message>(bytes, _jsonOptions);

            if (deserialized?.TimestampInTicks != null)
            {
                var timeToDeliver = DateTime.UtcNow.Ticks - deserialized.TimestampInTicks;
                var timeSpan = TimeSpan.FromTicks(Math.Max(timeToDeliver, 0));

                Metrics.DeliveryTime.Record(timeSpan.TotalMilliseconds);
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
