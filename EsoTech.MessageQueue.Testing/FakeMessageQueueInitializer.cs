using EsoTech.MessageQueue.Abstractions;
using System.Collections.Generic;

namespace EsoTech.MessageQueue.Testing
{
    public class FakeMessageQueueInitializer
    {
        public FakeMessageQueueInitializer(IEnumerable<IEventMessageHandler> eventHandlers, IEnumerable<ICommandMessageHandler> commandHandlers, FakeMessageQueue fakeMessageQueue)
        {
            fakeMessageQueue.AddEventHandlers(eventHandlers);
            fakeMessageQueue.AddCommandHandlers(commandHandlers);
        }
    }
}
