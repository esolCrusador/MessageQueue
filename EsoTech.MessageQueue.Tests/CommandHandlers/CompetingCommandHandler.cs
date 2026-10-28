using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.CommandHandlers
{
    class CompetingCommandHandler : ICommandMessageHandler<BroadcastCommandMsg>
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
