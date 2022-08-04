using System;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public static class MessageQueueExtensions
    {
        public static async Task HandleNext(this IMessageConsumer self)
        {
            using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await self.HandleNext(cancellationTokenSource.Token);
        }
    }
}
