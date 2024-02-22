using System;

namespace EsoTech.MessageQueue.Abstractions.Aggregations
{
    public interface IMessageQueueSession : IMessageQueue
    {
        void ClearEvents(Predicate<object> predicate);
        void ClearDelayedEvents(Predicate<object> predicate);
        void ClearCommands(Predicate<object> predicate);
    }
}
