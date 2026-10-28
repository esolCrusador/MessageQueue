using System;

namespace EsoTech.MessageQueue.Abstractions.Attributes
{
    /// <summary>
    /// Marks an event message handler as a broadcast subscription: every instance of the service
    /// gets its own queue and receives a copy of each event. Broadcast events are transient —
    /// instances that are down when an event is published miss it.
    /// Applies to all messages handled by the marked handler class; split the handler class
    /// if only some of its messages should be broadcast.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class BroadcastAttribute : Attribute
    {
    }
}
