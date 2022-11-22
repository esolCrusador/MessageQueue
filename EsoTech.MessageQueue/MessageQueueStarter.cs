using Microsoft.Extensions.Hosting;
using System.Threading;
using System.Threading.Tasks;
using EsoTech.MessageQueue.Abstractions;

namespace EsoTech.MessageQueue
{
    internal sealed class MessageQueueStarter : IHostedService
    {
        private readonly IMessageConsumer _consumer;

        public MessageQueueStarter(IMessageConsumer consumer)
        {
            _consumer = consumer;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await _consumer.Initialize(cancellationToken);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _consumer.DisposeAsync();
        }
    }
}
