using System;

namespace EsoTech.MessageQueue.Abstractions.Aggregations
{
    public class SimpleMessagesAggregator<TMessage, TAggregatedMessage> : MessagesAggregator<TMessage, TAggregatedMessage>
        where TAggregatedMessage : class, new()
    {
        private readonly Action<TAggregatedMessage, TMessage> _addMessage;

        public SimpleMessagesAggregator(Action<TAggregatedMessage, TMessage> addMessage)
        {
            _addMessage = addMessage;
        }
        public override void Aggregate(TAggregatedMessage aggregate, TMessage message) => _addMessage(aggregate, message);

        public override TAggregatedMessage CreateAggregator() => new TAggregatedMessage();
    }
}
