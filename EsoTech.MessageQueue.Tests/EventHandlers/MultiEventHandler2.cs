using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    class MultiEventHandler2 : IEventMessageHandler<MultiMsg>
    {
        private readonly List<MultiMsg> _log = new List<MultiMsg>();

        public IReadOnlyList<MultiMsg> Log => _log;

        public async Task Handle(MultiMsg msg, CancellationToken cancellationToken)
        {
            _log.Add(msg);
            await Task.CompletedTask;
        }
    }
}