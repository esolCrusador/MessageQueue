using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.EventHandlers
{
    internal class FooEventDelegateHandler : IEventMessageHandler<FooMsg>
    {
        private static readonly Func<FooMsg, CancellationToken, Task> DefaultHandler = (_, _) => Task.CompletedTask;
        private readonly List<FooMsg> _log = new List<FooMsg>();

        public IReadOnlyList<FooMsg> Log => _log;
        public Func<FooMsg, CancellationToken, Task> Handler = DefaultHandler;

        public async Task Handle(FooMsg eventMessage, CancellationToken cancellationToken)
        {
            await Handler(eventMessage, cancellationToken);
            _log.Add(eventMessage);
        }

        public void ResetHandler() => Handler = DefaultHandler;
    }
}
