using Azure;
using Azure.Messaging.ServiceBus;
using System;
using System.Net;

namespace EsoTech.MessageQueue.AzureServiceBus
{
    internal static class ExceptionExtensions
    {
        public static bool IsDuplicated(this ServiceBusException serviceBusException)
        {
            return (serviceBusException.InnerException as RequestFailedException)?.Status == (int)HttpStatusCode.Conflict
                && serviceBusException.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsNotFound(this Exception serviceBusException)
        {
            return (serviceBusException as ServiceBusException)?.Reason == ServiceBusFailureReason.MessagingEntityNotFound;
        }
    }
}
