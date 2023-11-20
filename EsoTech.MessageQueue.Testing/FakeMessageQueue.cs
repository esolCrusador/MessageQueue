using EsoTech.MessageQueue.Abstractions;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Testing
{
    public sealed class FakeMessageQueue : IMessageQueue, IMessageConsumer
    {
        private readonly bool _automaticPolling;
        private ILookup<Type, Func<object, CancellationToken, Task>> _handlers;
        private IEnumerable<IEventMessageHandler> _eventHandlerSources;
        private IEnumerable<ICommandMessageHandler> _commandHandleSources;

        public MessagesCollection Messages { get; } = new MessagesCollection();

        public FakeMessageQueue(ContinuousPollingSuppressor continuousPollingSuppressor) : this(false)
        { }

        public FakeMessageQueue() : this(true)
        { }

        private FakeMessageQueue(bool automaticPolling)
        {
            _eventHandlerSources = Enumerable.Empty<IEventMessageHandler>();
            _commandHandleSources = Enumerable.Empty<ICommandMessageHandler>();
            _handlers = GetHandlersLookup();
            _automaticPolling = automaticPolling;
        }

        private ILookup<Type, Func<object, CancellationToken, Task>> GetHandlersLookup()
        {
            var eventHandlers = _eventHandlerSources
                .SelectMany(h =>
                {
                    var type = h.GetType();
                    var interfaces = type.GetInterfaces()
                        .Where(t => t.Name == $"{nameof(IEventMessageHandler)}`1");

                    return interfaces.Select(i => new HandlerMetadata(i.GenericTypeArguments[0], i, h));
                });
            var commandHandlers = _commandHandleSources.SelectMany(h =>
            {
                var type = h.GetType();
                var interfaces = type.GetInterfaces()
                    .Where(t => t.Name == $"{nameof(ICommandMessageHandler)}`1");

                return interfaces.Select(i => new HandlerMetadata(i.GenericTypeArguments[0], i, h));
            });

            return commandHandlers.Concat(eventHandlers)
                .ToLookup(
                    h => h.TargetType,
                    h =>
                    {
                        return HandlerExtensions.CreateHandleDelegate(h.Instance, h.InterfaceType, "Handle");
                    });
        }

        private class HandlerMetadata
        {
            public Type TargetType { get; }
            public Type InterfaceType { get; }
            public object Instance { get; }

            public HandlerMetadata(Type targetType, Type interfaceType, object instance)
            {
                TargetType = targetType;
                InterfaceType = interfaceType;
                Instance = instance;
            }
        }

        internal void AddEventHandlers(IEnumerable<IEventMessageHandler> handlers)
        {
            _eventHandlerSources = _eventHandlerSources.Union(handlers);
            _handlers = GetHandlersLookup();
        }

        internal void AddCommandHandlers(IEnumerable<ICommandMessageHandler> handlers)
        {
            _commandHandleSources = _commandHandleSources.Union(handlers);
            _handlers = GetHandlersLookup();
        }

        public void AddEventHandler(IEventMessageHandler handler)
        {
            _eventHandlerSources = _eventHandlerSources.Append(handler);
            _handlers = GetHandlersLookup();
        }

        public void AddCommandHandler(ICommandMessageHandler handler)
        {
            _commandHandleSources = _commandHandleSources.Append(handler);
            _handlers = GetHandlersLookup();
        }

        public void RemoveEventHandler<THandler>()
            where THandler : IEventMessageHandler
        {
            var handlerType = typeof(THandler);
            _eventHandlerSources = _eventHandlerSources.Where(x => x.GetType() != handlerType).ToList();
            _handlers = GetHandlersLookup();
        }

        public void RemoveCommandHandler<THandler>()
            where THandler : ICommandMessageHandler
        {
            var handlerType = typeof(THandler);
            _eventHandlerSources = _eventHandlerSources.Where(x => x.GetType() != handlerType).ToList();
            _handlers = GetHandlersLookup();
        }

        public Task Initialize(CancellationToken cancellation)
        {
            return Task.CompletedTask;
        }

        public async Task HandleNext(CancellationToken cancellation)
        {
            await Task.Run(async () =>
            {
                var msg = Messages.Take(cancellation);

                if (cancellation.IsCancellationRequested)
                    throw new TaskCanceledException();

                var handlers = _handlers[msg.GetType()].ToList();

                if (handlers.Any())
                {
                    foreach (var handler in handlers)
                        await handler(msg, cancellation);
                }
                else
                    await HandleNext(cancellation);
            });
        }

        public async Task<bool> TryHandleNext(CancellationToken cancellationToken = default)
        {
            if (Messages.TryTake(out var msg))
            {
                var handlers = _handlers[msg.GetType()].ToList();

                if (!handlers.Any())
                    return await TryHandleNext(cancellationToken);

                foreach (var handler in handlers)
                    await handler(msg, cancellationToken);

                return true;
            }

            return false;
        }

        public async Task<int> HandleAll(CancellationToken cancellationToken = default)
        {
            int times = 0;
            while (await TryHandleNext(cancellationToken))
                times++;

            return times;
        }

        public async Task ProcessOnlyCurrentMessages()
        {
            await ProcessCurrentUntilFirstOfType<FakeMessageQueue>();
        }

        public async Task<bool> ProcessCurrentUntilFirstOfType<T>() where T : class
        {
            var currentMessagesCount = Messages.Count;
            for (var idx = 0; idx < currentMessagesCount; idx++)
            {
                var hasTargetType = Messages.FirstOrDefault() is T;
                if (hasTargetType) return true;

                if (Messages.TryTake(out var msg))
                {
                    var handlers = _handlers[msg.GetType()].ToList();

                    foreach (var handler in handlers)
                        await handler(msg, default);
                }
            }

            return false;
        }

        public ValueTask DisposeAsync() => default;

        public Task SendEvent(object eventMessage) => Send(eventMessage);
        public Task SendCommand(object commandMessage) => Send(commandMessage);

        private async Task Send(object msg)
        {
            Messages.AddMessage(msg);

            if (_automaticPolling)
                await HandleAll();
        }

        public async Task SendEvents(IEnumerable<object> eventMessages)
        {
            foreach (var msg in eventMessages)
                Messages.AddMessage(msg);

            if (_automaticPolling)
                await HandleAll();
        }
    }
}
