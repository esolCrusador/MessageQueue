using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    class BarEventHandler : IEventMessageHandler<BarMsg>, IEventMessageHandler<NeverSentMsg>
    {
        private readonly List<BarMsg> _log = new List<BarMsg>();

        public IReadOnlyList<BarMsg> Log => _log;

        public Task Handle(BarMsg msg, CancellationToken cancellationToken)
        {
            _log.Add(msg);
            return Task.CompletedTask;
        }

        // This handler is added to make sure that we can deal with multiple IMessageHandler implementations in a single class.
        public Task Handle(NeverSentMsg msg, CancellationToken cancellationToken)
        {
            throw new NotImplementedException("This method should be never invoked.");
        }
    }
}
