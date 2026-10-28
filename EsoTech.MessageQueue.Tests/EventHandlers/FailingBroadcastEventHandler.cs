using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Abstractions.Attributes;
using EsoTech.MessageQueue.Tests.Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    [Broadcast]
    class FailingBroadcastEventHandler : IEventMessageHandler<BroadcastMsg>
    {
        private readonly List<BroadcastMsg> _log = new List<BroadcastMsg>();

        public IReadOnlyList<BroadcastMsg> Log => _log;

        public int FailTimes { get; set; }

        public int Attempts { get; private set; }

        public Task Handle(BroadcastMsg msg, CancellationToken cancellationToken)
        {
            Attempts++;
            if (Attempts <= FailTimes)
                throw new InvalidOperationException("Intentional failure for testing redelivery");

            _log.Add(msg);
            return Task.CompletedTask;
        }
    }
}
