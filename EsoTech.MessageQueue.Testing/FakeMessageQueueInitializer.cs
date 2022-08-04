using EsoTech.MessageQueue.Abstractions;
using System.Collections.Generic;

namespace EsoTech.MessageQueue.Testing
{
    public class FakeMessageQueueInitializer
    {
        public FakeMessageQueueInitializer(IEnumerable<IMessageHandler> handlers, FakeMessageQueue fakeMessageQueue) =>
            fakeMessageQueue.AddHandlers(handlers);
    }
}
