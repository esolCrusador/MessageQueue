﻿using EsoTech.MessageQueue.Abstractions;
using EsoTech.MessageQueue.Tests.Messages;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Tests.CommandHandlers
{
    class FooCommandHandler : ICommandMessageHandler<FooMsg>
    {
        private readonly List<FooMsg> _log = new List<FooMsg>();

        public IReadOnlyList<FooMsg> Log => _log;

        public async Task Handle(FooMsg msg, CancellationToken cancellationToken)
        {
            _log.Add(msg);
            await Task.CompletedTask;
        }
    }
}
