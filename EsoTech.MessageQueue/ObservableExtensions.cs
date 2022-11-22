using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace EsoTech.MessageQueue
{
    internal static class ObservableExtensions
    {
        public static IObservable<TElement> PostponeNotHandled<TElement>(this IObservable<TElement> elements, int maxQueueSize = int.MaxValue)
        {
            var queue = new Queue<TElement>();
            var results = new BehaviorSubject<Unit>(Unit.Default);

            IDisposable? subscription = null;
            subscription = elements.Subscribe(
                e =>
                {
                    lock (queue)
                    {
                        queue.Enqueue(e);
                        if (queue.Count > maxQueueSize)
                            queue.Dequeue();
                    }

                    results.OnNext(Unit.Default);
                },
                exception => results.OnError(exception),
                () =>
                {
                    queue.Clear();
                    subscription?.Dispose();
                });

            return results.Select(_ =>
            {
                return queue.DequeueAll();
            }).Merge();
        }

        public static IObservable<TElement> DequeueAll<TElement>(this Queue<TElement> queue)
        {
            if (queue.Count == 0)
                return Observable.Empty<TElement>();

            List<TElement> results = new List<TElement>(queue.Count);
            lock (queue)
            {
                while (queue.TryDequeue(out var element))
                    results.Add(element);
            }

            return results.ToObservable();
        }
    }
}
