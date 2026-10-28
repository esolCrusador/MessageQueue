using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Abstractions.Attributes;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.CommandHandlers
{
    [Broadcast]
    class BroadcastCommandHandler1 : ICommandMessageHandler<BroadcastCommandMsg>
    {
        private readonly List<BroadcastCommandMsg> _log = new List<BroadcastCommandMsg>();

        public IReadOnlyList<BroadcastCommandMsg> Log => _log;

        public async Task Handle(BroadcastCommandMsg commandMessage, CancellationToken cancellationToken)
        {
            _log.Add(commandMessage);
            await Task.CompletedTask;
        }
    }
}
