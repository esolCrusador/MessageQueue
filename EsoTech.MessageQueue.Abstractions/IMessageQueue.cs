using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface IMessageQueue
    {
        Task Send(object msg);
    }
}
