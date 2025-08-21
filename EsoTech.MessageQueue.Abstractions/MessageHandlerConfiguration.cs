using System;

namespace EsoTech.MessageQueue.Abstractions
{
    public class MessageHandlerConfiguration
    {
        public Type MessageType { get; }
        public Type HandlerType { get; }
        public int? MaxConcurrentMessages { get; set; }

        public MessageHandlerConfiguration(Type messageType, Type handlerType)
        {
            MessageType = messageType;
            HandlerType = handlerType;
        }
    }
}
