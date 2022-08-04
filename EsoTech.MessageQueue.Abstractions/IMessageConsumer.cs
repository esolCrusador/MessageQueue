using System;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface IMessageConsumer : IAsyncDisposable
    {
        Task Initialize(CancellationToken cancellation);
        Task HandleNext(CancellationToken cancellation);
        Task<bool> TryHandleNext(CancellationToken cancellation = default);
    }
}
