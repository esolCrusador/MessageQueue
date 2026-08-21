using System.Diagnostics.Metrics;

namespace EsoTech.MessageQueue
{
    public static class Metrics
    {
        private static readonly Meter Meter = new Meter("EsoTech.MessageQueue");

        public static readonly Histogram<double> DeliveryTime =
            Meter.CreateHistogram<double>(
                "message_queue_delivery_time_milliseconds",
                "milliseconds",
                "Time in flight for message queue messages.");

        public static readonly Counter<long> ExecutedMessages =
            Meter.CreateCounter<long>(
                "service_bus_executed_messages_amount",
                "{message}",
                "Amount of successfully executed messages.");

        public static readonly Counter<long> FailedMessages =
            Meter.CreateCounter<long>(
                "service_bus_failed_messages_amount",
                "{message}",
                "Amount of failed messages.");

        public static readonly Histogram<double> HandlingDuration =
            Meter.CreateHistogram<double>(
                "service_bus_messages_durations_milliseconds",
                "milliseconds",
                "Duration of performed messages.");
    }
}
