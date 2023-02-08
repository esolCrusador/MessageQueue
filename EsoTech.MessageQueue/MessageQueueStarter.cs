using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.Hosting;
using System.Threading;
using System.Threading.Tasks;

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

        public Task StopAsync(CancellationToken cancellationToken) =>
            Task.CompletedTask; // _consumer is disposed by ServiceProvider
    }
}
