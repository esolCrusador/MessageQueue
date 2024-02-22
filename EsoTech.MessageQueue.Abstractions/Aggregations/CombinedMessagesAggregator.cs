using System.Collections.Generic;
using System.Linq;

namespace EsoTech.MessageQueue.Abstractions.Aggregations
{
    public class CombinedMessagesAggregator
    {
        private readonly IReadOnlyList<IMessagesAggregator> _aggregators;

        public CombinedMessagesAggregator(IEnumerable<IMessagesAggregator> aggregators)
        {
            _aggregators = aggregators.ToArray();
        }

        public List<object> Aggregate(IEnumerable<object> messages)
        {
            object[] aggregations = new object[_aggregators.Count];
            bool anyAggregated = false;
            var result = messages.ToList();

            foreach (var message in messages)
                for (int i = 0; i < _aggregators.Count; i++)
                {
                    var aggregator = _aggregators[i];

                    if (!aggregator.CanAggregate(message))
                        continue;
                    anyAggregated = true;

                    var previous = aggregations[i];
                    aggregations[i] = aggregator.Aggregate(previous, message);
                    result.Remove(message);
                }

            if (anyAggregated)
                result.AddRange(aggregations.Where(agg => agg != null));

            return result;
        }
    }

}
