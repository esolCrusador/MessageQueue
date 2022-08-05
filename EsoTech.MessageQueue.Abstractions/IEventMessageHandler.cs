using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface IEventMessageHandler<TEvent> : IEventMessageHandler
    {
        Task Handle(TEvent eventMessage, CancellationToken cancellationToken);
    }

    public interface IEventMessageHandler
    {
    }
}
