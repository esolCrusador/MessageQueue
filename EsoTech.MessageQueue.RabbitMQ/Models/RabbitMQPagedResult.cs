using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace EsoTech.MessageQueue.RabbitMQ.Models
{
    public class RabbitMQPagedResult<TItem>
    {
        [JsonPropertyName("filtered_count")] public int FilteredCount { get; set; }
        [JsonPropertyName("item_count")] public int ItemCount { get; set; }
        [JsonPropertyName("items")] public List<TItem>? Items { get; set; }
        [JsonPropertyName("page")] public int Page { get; set; }
        [JsonPropertyName("page_count")] public int PageCount { get; set; }
        [JsonPropertyName("page_size")] public int PageSize { get; set; }
        [JsonPropertyName("total_count")] public int TotalCount { get; set; }
    }
}
