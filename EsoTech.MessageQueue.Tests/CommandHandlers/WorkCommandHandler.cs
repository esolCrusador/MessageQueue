using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.CommandHandlers
{
    class WorkCommandHandler : ICommandMessageHandler<WorkCommandMsg>
    {
        private readonly List<WorkCommandMsg> _log = new List<WorkCommandMsg>();

        public IReadOnlyList<WorkCommandMsg> Log => _log;

        public async Task Handle(WorkCommandMsg commandMessage, CancellationToken cancellationToken)
        {
            _log.Add(commandMessage);
            await Task.CompletedTask;
        }
    }
}
