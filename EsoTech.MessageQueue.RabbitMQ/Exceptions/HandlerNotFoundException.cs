using System;

namespace EsoTech.MessageQueue.RabbitMQ.Exceptions
{
    public class HandlerNotFoundException : Exception
    {
        public HandlerNotFoundException(string messageType, string messageString) : base($"Message handler for type {messageType} was not found. Removing routing key for message {messageString}")
        {
        }
    }
}
