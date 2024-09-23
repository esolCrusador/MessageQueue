using EsoTech.MessageQueue.Abstractions;
using FluentAssertions;
using System;
using System.Collections.Concurrent;
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

        public FakeMessageQueue(bool automaticPolling)
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

        private ConcurrentDictionary<string, Func<object, CancellationToken, Task>[]> _failedHandlers = new ConcurrentDictionary<string, Func<object, CancellationToken, Task>[]>();
        public async Task HandleNext(CancellationToken cancellationToken)
        {
            await Task.Run(async () =>
            {
                var msg = Messages.Take(cancellationToken);

                cancellationToken.ThrowIfCancellationRequested();

                var messageId = msg!.Headers["MessageId"];
                if (_failedHandlers.TryGetValue(messageId, out var handlers))
                    _failedHandlers.Remove(messageId, out _);
                else
                    handlers = _handlers[msg!.Payload!.GetType()].ToArray();

                if (!handlers.Any())
                    await HandleNext(cancellationToken);

                var handlerTasks = handlers.Select(h => h(msg.Payload!, cancellationToken)).ToList();
                try
                {
                    await Task.WhenAll(handlerTasks);
                }
                catch
                {
                    _failedHandlers[msg.Headers["MessageId"]] = handlers.Zip(
                        handlerTasks, (handler, handlerTask) => new { Handler = handler, HandlerTask = handlerTask }
                    ).Where(kvp => kvp.HandlerTask.IsFaulted)
                    .Select(kvp => kvp.Handler)
                    .ToArray();
                    throw;
                }
            });
        }

        public async Task<bool> TryHandleNext(CancellationToken cancellationToken = default)
        {
            if (Messages.TryTake(out var msg))
            {
                var messageId = msg!.Headers["MessageId"];
                if (_failedHandlers.TryGetValue(messageId, out var handlers))
                    _failedHandlers.Remove(messageId, out _);
                else
                    handlers = _handlers[msg!.Payload!.GetType()].ToArray();

                if (!handlers.Any())
                    return await TryHandleNext(cancellationToken);

                var handlerTasks = handlers.Select(h => h(msg.Payload!, cancellationToken)).ToList();
                try
                {
                    await Task.WhenAll(handlerTasks);
                }
                catch
                {
                    _failedHandlers[msg.Headers["MessageId"]] = handlers.Zip(
                        handlerTasks, (handler, handlerTask) => new { Handler = handler, HandlerTask = handlerTask }
                    ).Where(kvp => kvp.HandlerTask.IsFaulted)
                    .Select(kvp => kvp.Handler)
                    .ToArray();
                    throw;
                }

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

        public ValueTask DisposeAsync() => default;

        public Task SendEvent(object eventMessage, TimeSpan? delay = default) => Send(eventMessage, delay);
        public Task SendCommand(object commandMessage) => Send(commandMessage);

        private async Task Send(object msg, TimeSpan? delay = default)
        {
            if (delay.HasValue)
            {
                var _ = SendWithDelay(msg, delay.Value);
            }
            else
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

        public async Task SendCommands(IEnumerable<object> commands)
        {
            foreach (var cmd in commands)
                Messages.AddMessage(cmd);

            if (_automaticPolling)
                await HandleAll();
        }

        private async Task SendWithDelay(object msg, TimeSpan delay)
        {
            await Task.Delay(delay);
            await Send(msg);
        }
    }
}
