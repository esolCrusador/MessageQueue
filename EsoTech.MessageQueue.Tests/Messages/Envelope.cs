namespace EsoTech.MessageQueue.Tests.Messages
{
    class Envelope<T>
    {
        public T? Payload { get; set; }
    }
}
