using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    class EnvelopedFooEventHandler : IEventMessageHandler<Envelope<FooMsg>>
    {
        private readonly List<Envelope<FooMsg>> _log = new List<Envelope<FooMsg>>();

        public IReadOnlyList<Envelope<FooMsg>> Log => _log;

        public async Task Handle(Envelope<FooMsg> msg, CancellationToken cancellationToken)
        {
            _log.Add(msg);
            await Task.CompletedTask;
        }
    }
}
