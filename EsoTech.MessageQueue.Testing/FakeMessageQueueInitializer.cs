using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.Hosting;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Testing
{
    public class FakeMessageQueueInitializer: IHostedService
    {
        public FakeMessageQueueInitializer(IEnumerable<IEventMessageHandler> eventHandlers, IEnumerable<ICommandMessageHandler> commandHandlers, FakeMessageQueue fakeMessageQueue)
        {
            fakeMessageQueue.AddEventHandlers(eventHandlers);
            fakeMessageQueue.AddCommandHandlers(commandHandlers);
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
