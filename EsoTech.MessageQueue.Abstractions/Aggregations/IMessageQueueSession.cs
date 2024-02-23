using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions.Aggregations
{
    public interface IMessageQueueSession
    {
        void SendEvent(object eventMessage, TimeSpan? delay = default);
        void SendEvents(IEnumerable<object> eventMessages);
        void SendCommand(object commandMessage);
        void ClearEvents(Predicate<object> predicate);
        void ClearDelayedEvents(Predicate<object> predicate);
        void ClearCommands(Predicate<object> predicate);
        ValueTask FlushEvents();
    }
}
