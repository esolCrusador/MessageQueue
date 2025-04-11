using System;

namespace EsoTech.MessageQueue.Abstractions
{
    public interface ILongRunningHandler<TMessage>
    {
        public TimeSpan? GetTimeout(TMessage message);
    }
}
