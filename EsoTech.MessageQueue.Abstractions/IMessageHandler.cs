using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface IMessageHandler<T> : IMessageHandler
    {
        Task Handle(T msg, CancellationToken cancellationToken);
    }

    public interface IMessageHandler
    {
    }
}
