using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    class FooEventHandler : IEventMessageHandler<FooMsg>, IEventMessageHandler<FooDelayedMessage>
    {
        private readonly List<FooMsg> _log = new List<FooMsg>();
        private readonly List<FooDelayedMessage> _delayedLog = new List<FooDelayedMessage>();

        public IReadOnlyList<FooMsg> Log => _log;
        public IReadOnlyList<FooDelayedMessage> DelayedLog => _delayedLog;

        public async Task Handle(FooMsg msg, CancellationToken cancellationToken)
        {
            _log.Add(msg);
            await Task.CompletedTask;
        }

        public Task Handle(FooDelayedMessage eventMessage, CancellationToken cancellationToken)
        {
            _delayedLog.Add(eventMessage);
            return Task.CompletedTask;
        }
    }
}
