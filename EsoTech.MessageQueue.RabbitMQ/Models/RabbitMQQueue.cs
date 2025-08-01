using System.Text.Json.Serialization;

namespace EsoTech.MessageQueue.RabbitMQ.Models
{
    public class RabbitMQQueue
    {
        [JsonPropertyName("name")] public required string Name { get; set; }
        [JsonPropertyName("vhost")] public required string VirtualHost { get; set; }
        [JsonPropertyName("consumers")] public int ConsumersCount { get; set; }
        [JsonPropertyName("messages")] public int MessagesCount { get; set; }
        [JsonPropertyName("type")] public required string Type { get; set; }
    }
}
