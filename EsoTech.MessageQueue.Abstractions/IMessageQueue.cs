using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface IMessageQueue
    {
        Task SendEvent(object eventMessage, TimeSpan? delay = default);
        Task SendEvents(IEnumerable<object> eventMessages);
        Task SendCommand(object commandMessage);
        Task SendCommands(IEnumerable<object> commands);
    }
}
