using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Abstractions.Attributes;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    [Broadcast]
    class BroadcastEventHandler2 : IEventMessageHandler<BroadcastMsg>
    {
        private readonly List<BroadcastMsg> _log = new List<BroadcastMsg>();

        public IReadOnlyList<BroadcastMsg> Log => _log;

        public async Task Handle(BroadcastMsg msg, CancellationToken cancellationToken)
        {
            _log.Add(msg);
            await Task.CompletedTask;
        }
    }
}
