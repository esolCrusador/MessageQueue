using System.Diagnostics.Metrics;

namespace EsoTech.MessageQueue
{
    public static class Metrics
    {
        private static readonly Meter Meter = new Meter("EsoTech.MessageQueue");

        public static readonly Histogram<double> DeliveryTime =
            Meter.CreateHistogram<double>("message_queue_delivery_time_milliseconds", "Time in flight for message queue messages.");

        private static readonly Counter<long> ExecutedMessages =
            Meter.CreateCounter<long>("service_bus_executed_messages_amount", "Currently executed amount of messages", "event_name", "queue_name");

        private static readonly Counter<long> FailedMessages =
            Meter.CreateCounter<long>("service_bus_failed_messages_amount", "Current failed amount of messages", "event_name", "queue_name");

        private static readonly Histogram<double> MessagesDurations =
            Meter.CreateHistogram<double>("service_bus_messages_durations_seconds", "Durations of performed messages", "event_name", "queue_name");
    }
}
