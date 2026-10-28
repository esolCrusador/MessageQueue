using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    class WorkEventHandler : IEventMessageHandler<WorkMsg>
    {
        private readonly List<WorkMsg> _log = new List<WorkMsg>();

        public IReadOnlyList<WorkMsg> Log => _log;

        public async Task Handle(WorkMsg msg, CancellationToken cancellationToken)
        {
            _log.Add(msg);
            await Task.CompletedTask;
        }
    }
}
