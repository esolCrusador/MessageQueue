using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface ICommandMessageHandler<TCommand> : ICommandMessageHandler
    {
        Task Handle(TCommand eventMessage, CancellationToken cancellationToken);
    }

    public interface ICommandMessageHandler
    {
    }
}
