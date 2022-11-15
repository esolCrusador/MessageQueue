using System.Collections.Generic;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface IMessageQueue
    {
        Task SendEvent(object eventMessage);
        Task SendEvents(IEnumerable<object> eventMessages);
        Task SendCommand(object commandMessage);
    }
}
