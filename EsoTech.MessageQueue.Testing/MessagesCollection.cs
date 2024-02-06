using EsoTech.MessageQueue.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;

namespace EsoTech.MessageQueue.Testing
{
    public class MessagesCollection
    {
        public static readonly ILogger<MessageSerializer> Logger = LoggerFactory.Create(cfg =>
        {
            cfg.AddConsole();
        }).CreateLogger<MessageSerializer>();

        private readonly MessageSerializer _serializer;
        private readonly BlockingCollection<byte[]> _messages;

        public int Count => _messages.Count;

        public MessagesCollection()
        {
            _serializer = new MessageSerializer(new LoggerFactory().CreateLogger<MessageSerializer>());
            _messages = new BlockingCollection<byte[]>();
        }

        public void AddMessage(object msg)
        {
            _messages.Add(_serializer.Serialize(new Message
            {
                PayloadTypeName = msg.GetType().AssemblyQualifiedName,
                Payload = msg,
                TimestampInTicks = DateTimeOffset.UtcNow.Ticks
            }));
        }

        public object Take(CancellationToken cancellation)
        {
            byte[] bytes = _messages.Take(cancellation);
            return _serializer.Deserialize(bytes).Payload ?? throw new ArgumentException("Null", nameof(Message.Payload));
        }

        public object? FirstOrDefault()
        {
            var bytes = _messages.FirstOrDefault();
            return bytes == null ? null : _serializer.Deserialize(bytes).Payload;
        }

        public bool TryTake([MaybeNullWhen(false)] out object? msg)
        {
            bool suceeded = _messages.TryTake(out var bytes);
            msg = suceeded ? _serializer.Deserialize(bytes).Payload : null;
            return suceeded;
        }
    }
}
