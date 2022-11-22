using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace EsoTech.MessageQueue.Serialization
{
    internal class MessageConverter : JsonConverter<Message>
    {
        delegate object Converter(ref Utf8JsonReader reader, JsonSerializerOptions options);
        private readonly ConcurrentDictionary<Guid, Converter> _converters = new ConcurrentDictionary<Guid, Converter>();

        public override Message Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var message = new Message();
            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName && reader.GetString() != nameof(Message.PayloadTypeName))
                throw new System.ArgumentException("Message was not properly serialized");
            reader.Read();
            message.PayloadTypeName = reader.GetString();

            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName && reader.GetString() != nameof(Message.Headers))
                throw new System.ArgumentException("Message was not properly serialized");
            reader.Read();
            var dictConverter = (JsonConverter<Dictionary<string, string>>)options.GetConverter(typeof(Dictionary<string, string>));
            message.Headers = dictConverter.Read(ref reader, typeof(Dictionary<string, string>), options);

            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName && reader.GetString() != nameof(Message.TimestampInTicks))
                throw new System.ArgumentException("Message was not properly serialized");
            reader.Read();
            message.TimestampInTicks = reader.GetInt64();

            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName && reader.GetString() != nameof(Message.Payload))
                throw new System.ArgumentException("Message was not properly serialized");
            reader.Read();
            var payloadType = Type.GetType(message.PayloadTypeName);
            var converter = options.GetConverter(payloadType);
            var convert = _converters.GetOrAdd(payloadType.GUID, _ => CreateTypeConverter(converter, payloadType));
            message.Payload = convert(ref reader, options);
            reader.Read();

            return message;
        }

        public override void Write(Utf8JsonWriter writer, Message value, JsonSerializerOptions options)
        {
            value.PayloadTypeName = value.Payload?.GetType().AssemblyQualifiedName;
            JsonSerializer.Serialize(writer, value);
        }

        private Converter CreateTypeConverter(JsonConverter converter, Type payloadType)
        {
            var converterType = typeof(JsonConverter<>).MakeGenericType(payloadType);
            MethodInfo method = converterType.GetMethod("Read");

            ConstantExpression converterInstance = Expression.Constant(converter);
            ParameterExpression refReader = Expression.Parameter(typeof(Utf8JsonReader).MakeByRefType(), "reader");
            ConstantExpression typeToConvert = Expression.Constant(payloadType);
            ParameterExpression options = Expression.Parameter(typeof(JsonSerializerOptions), "options");

            return Expression.Lambda<Converter>(
                Expression.Call(
                    Expression.Convert(converterInstance, converterType),
                    method,
                    refReader,
                    typeToConvert,
                    options
                ),
                refReader,
                options
            ).Compile();
        }
    }
}
