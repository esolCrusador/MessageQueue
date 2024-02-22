using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests
{
    public class SessionMessageHandler : IEventMessageHandler<SessionMsg>, IEventMessageHandler<List<SessionMsg>>
    {
        private readonly List<SessionMsg> _log = new List<SessionMsg>();
        private readonly List<List<SessionMsg>> _aggregatedLog = new List<List<SessionMsg>>();

        public IReadOnlyList<SessionMsg> Log => _log;
        public IReadOnlyList<List<SessionMsg>> AggregatedLog => _aggregatedLog;
        public Task Handle(SessionMsg eventMessage, CancellationToken cancellationToken)
        {
            _log.Add(eventMessage);

            return Task.CompletedTask;
        }

        public Task Handle(List<SessionMsg> eventMessage, CancellationToken cancellationToken)
        {
            _aggregatedLog.Add(eventMessage);

            return Task.CompletedTask;
        }
    }
}
