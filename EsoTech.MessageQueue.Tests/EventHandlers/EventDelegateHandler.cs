using EsoTech.MessageQueue.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    internal class EventDelegateHandler<TMessage> : IEventMessageHandler<TMessage>
    {
        private static readonly Func<TMessage, CancellationToken, Task> DefaultHandler = (_, _) => Task.CompletedTask;
        private readonly List<TMessage> _log = new List<TMessage>();

        public IReadOnlyList<TMessage> Log => _log;
        public Func<TMessage, CancellationToken, Task> Handler = DefaultHandler;

        public async Task Handle(TMessage eventMessage, CancellationToken cancellationToken)
        {
            await Handler(eventMessage, cancellationToken);
            _log.Add(eventMessage);
        }

        public void ResetHandler() => Handler = DefaultHandler;
    }
}
