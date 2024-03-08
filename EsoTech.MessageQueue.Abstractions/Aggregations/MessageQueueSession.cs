using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions.Aggregations
{
    public class MessageQueueSession : IMessageQueueSession, IAsyncDisposable
    {
        private readonly IMessageQueue _messageQueue;
        private readonly CombinedMessagesAggregator _aggregator;
        private List<object>? _commands;
        private List<object>? _events;
        private List<KeyValuePair<object, TimeSpan?>>? _delayedEvents;

        private List<object> Commands
        {
            get
            {
                if (_commands == null)
                    lock (this)
                        _commands ??= new List<object>();

                return _commands;
            }
        }

        private List<object> Events
        {
            get
            {
                if (_events == null)
                    lock (this)
                        _events ??= new List<object>();

                return _events;
            }
        }

        private List<KeyValuePair<object, TimeSpan?>> DelayedEvents
        {
            get
            {
                if (_delayedEvents == null)
                    lock (this)
                        _delayedEvents ??= new List<KeyValuePair<object, TimeSpan?>>();

                return _delayedEvents;
            }
        }

        public MessageQueueSession(IMessageQueue messageQueue, CombinedMessagesAggregator aggregator)
        {
            _messageQueue = messageQueue;
            _aggregator = aggregator;
        }

        public ValueTask DisposeAsync() => FlushMessages();

        public void SendCommand(object commandMessage)
        {
            lock (Commands)
                Commands.Add(commandMessage);
        }

        public void SendEvent(object eventMessage, TimeSpan? delay = null)
        {
            if (delay == null)
                lock (Events)
                    Events.Add(eventMessage);
            else
                lock (DelayedEvents)
                    DelayedEvents.Add(new KeyValuePair<object, TimeSpan?>(eventMessage, delay));
        }

        public void SendEvents(IEnumerable<object> eventMessages)
        {
            lock (Events)
                Events.AddRange(eventMessages);
        }
        public void ClearEvents(Predicate<object> predicate) => _events?.RemoveAll(predicate);

        public void ClearDelayedEvents(Predicate<object> predicate) => _delayedEvents?.RemoveAll(kvp => predicate(kvp.Key));

        public void ClearCommands(Predicate<object> predicate) => _commands?.RemoveAll(predicate);

        public async ValueTask FlushMessages()
        {
            if (
                (_commands == null || _commands.Count == 0)
                && (_delayedEvents == null || _delayedEvents.Count == 0)
                && (_events == null || _events.Count == 0)
            )
                return;

            Task? task = null;
            List<Task>? tasks = null;

            lock (this)
            {
                if (_commands != null)
                {
                    if (task != null)
                        tasks = new List<Task>();

                    task = _commands.Count == 1
                        ? _messageQueue.SendCommand(_commands[0])
                        : Task.WhenAll(_commands.Select(c => _messageQueue.SendCommand(c)));
                    tasks?.Add(task);
                }

                if (_events != null)
                {
                    if (task != null)
                        tasks = new List<Task>();

                    task = _events.Count == 1
                        ? _messageQueue.SendEvent(_events[0])
                        : _messageQueue.SendEvents(_aggregator.Aggregate(_events));
                    tasks?.Add(task);
                }

                if (_delayedEvents != null)
                {
                    if (task != null)
                        tasks = new List<Task>();

                    task = _delayedEvents.Count == 1
                        ? _messageQueue.SendEvent(_delayedEvents[0].Key, _delayedEvents[0].Value)
                        : Task.WhenAll(_delayedEvents.Select(kvp => _messageQueue.SendEvent(kvp.Key, kvp.Value)));
                    tasks?.Add(task);
                }

                _commands?.Clear();
                _events?.Clear();
                _delayedEvents?.Clear();
            }

            if (tasks != null)
                await Task.WhenAll(tasks);
            else if (task != null)
                await task;
        }
    }
}
