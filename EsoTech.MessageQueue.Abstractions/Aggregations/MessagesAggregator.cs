namespace EsoTech.MessageQueue.Abstractions.Aggregations
{
    public abstract class MessagesAggregator<TMessage, TAggregatedMessage> : IMessagesAggregator
        where TAggregatedMessage : class
    {
        public object Aggregate(object? previous, object message)
        {
            if (previous == null)
                return message;

            if (previous is TAggregatedMessage aggregated)
                Aggregate(aggregated, (TMessage)message);
            else
            {
                aggregated = CreateAggregator();
                Aggregate(aggregated, (TMessage)previous);
                Aggregate(aggregated, (TMessage)message);
            }

            return aggregated!;
        }

        public bool CanAggregate(object ev) => ev is TMessage message && CanAggregate(message);

        public abstract TAggregatedMessage CreateAggregator();
        public abstract void Aggregate(TAggregatedMessage aggregate, TMessage message);
        public virtual bool CanAggregate(TMessage message) => true;
    }
}
