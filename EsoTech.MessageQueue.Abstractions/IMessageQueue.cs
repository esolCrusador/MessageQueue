using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface IMessageQueue
    {
        Task SendEvent(object eventMessage);
        Task SendCommand(object commandMessage);
    }
}
