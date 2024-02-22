namespace EsoTech.MessageQueue.Abstractions.Aggregations
{
    public interface IMessagesAggregator
    {
        bool CanAggregate(object ev);
        object Aggregate(object? agg, object ev);
    }
}
