using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using static System.Diagnostics.DistributedContextPropagator;

namespace EsoTech.MessageQueue
{
    public static class MessageQueueTracing
    {
        private static readonly DistributedContextPropagator Propagator = DistributedContextPropagator.Current;
        private static readonly ActivitySource ActivitySource = new ActivitySource("EsoTech.MessageQueue");
        public static string ActivitySourceName => ActivitySource.Name;

        public static Activity? StartSenderActivity(string messageInfo, string destanation)
        {
            var activity = ActivitySource.StartActivity(messageInfo, ActivityKind.Producer);
            activity?.AddTag("mq.queue", destanation);

            return activity;
        }

        public static void EnrichSenderActivity(Type type, string serializedMessage)
        {
            var activity = Activity.Current;
            activity?.AddTag("mq.message_name", type.Name);
            activity?.AddTag("mq.message", serializedMessage);
        }

        public static void EnrichSenderHeaders<TValue>(Dictionary<string, TValue> headers)
        {
            PropagatorSetterCallback setterCallback = headers switch
            {
                Dictionary<string, string> strs => AddStringHeaders,
                Dictionary<string, object?> objs => AddObjectHeaders,
                _ => throw new NotSupportedException($"Not supported {nameof(headers)} type {headers.GetType().FullName}")
            };
            var activity = Activity.Current;
            Propagator.Inject(activity ?? Activity.Current, headers, setterCallback);
        }

        public static Activity? StartConsumerActivity<TValue>(string messageInfo, IDictionary<string, TValue>? headers, string queueName, string messageText)
        {
            var activity = ActivitySource.StartActivity(messageInfo, ActivityKind.Consumer, ExtractContext(headers));
            activity?.AddTag("mq.queue", queueName);
            activity?.AddTag("mq.message", messageText);

            return activity;
        }

        private static void AddStringHeaders(object? carrier, string fieldName, string fieldValue)
        {
            var target = (IDictionary<string, string?>)carrier!;
            target[fieldName] = fieldValue;
        }

        private static void AddObjectHeaders(object? carrier, string fieldName, string fieldValue)
        {
            var target = (IDictionary<string, object?>)carrier!;
            target[fieldName] = Encoding.UTF8.GetBytes(fieldValue);
        }

        private static ActivityContext ExtractContext<TValue>(IDictionary<string, TValue>? headers)
        {
            if (headers is null || headers.Count == 0)
                return default;

            PropagatorGetterCallback getterCallback = headers switch
            {
                IDictionary<string, string> strs => ExtractStringValue,
                IDictionary<string, object?> objs => ExtractObjectValue,
                _ => throw new NotSupportedException($"Not supported {nameof(headers)} type {headers.GetType().FullName}")
            };
            Propagator.ExtractTraceIdAndState(headers, getterCallback, out var traceParent, out var traceState);

            return ActivityContext.TryParse(traceParent, traceState, out var context)
                ? context
                : default;
        }

        private static void ExtractStringValue(object? carrier, string fieldName, out string? fieldValue, out IEnumerable<string>? fieldValues)
        {
            fieldValue = null;
            fieldValues = null;
            var source = (IDictionary<string, string>)carrier!;

            source.TryGetValue(fieldName, out fieldValue);
        }

        private static void ExtractObjectValue(object? carrier, string fieldName, out string? fieldValue, out IEnumerable<string>? fieldValues)
        {
            fieldValue = null;
            fieldValues = null;
            var source = (IDictionary<string, object?>)carrier!;

            if (source.TryGetValue(fieldName, out var raw) && raw != null)
            {
                fieldValue = raw switch
                {
                    byte[] bytes => Encoding.UTF8.GetString(bytes),
                    string s => s,
                    _ => raw.ToString()
                };
            }
        }
    }
}
