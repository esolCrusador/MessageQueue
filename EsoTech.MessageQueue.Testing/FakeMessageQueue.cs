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
        private ILookup<Type, Func<object, CancellationToken, Task>> _handlers;

        private IEnumerable<IMessageHandler> _handlerSources;

        public BlockingCollection<object> Messages { get; } = new BlockingCollection<object>();

        public FakeMessageQueue()
        {
            _handlerSources = Enumerable.Empty<IMessageHandler>();
            _handlers = GetHandlersLookup();
        }

        private ILookup<Type, Func<object, CancellationToken, Task>> GetHandlersLookup() =>
            _handlerSources
                .SelectMany(h =>
                {
                    var type = h.GetType();
                    var interfaces = type.GetInterfaces()
                        .Where(t => t.Name == $"{nameof(IMessageHandler)}`1");

                    return interfaces.Select(i => new
                    {
                        TargetType = i.GenericTypeArguments[0],
                        InterfaceType = i,
                        Instance = h
                    });
                })
                .ToLookup(
                    h => h.TargetType,
                    h =>
                    {
                        return HandlerExtensions.CreateHandleDelegate(h.Instance, h.InterfaceType, "Handle");
                    });

        internal void AddHandlers(IEnumerable<IMessageHandler> handlers)
        {
            _handlerSources = _handlerSources.Union(handlers);
            _handlers = GetHandlersLookup();
        }

        public void AddHandler(IMessageHandler handler)
        {
            _handlerSources = _handlerSources.Append(handler);
            _handlers = GetHandlersLookup();
        }

        public void RemoveHandler(Type handlerType)
        {
            _handlerSources = _handlerSources.Where(x => x.GetType() != handlerType).ToList();
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

        public Task Send(object msg)
        {
            Messages.Add(msg);
            return Task.CompletedTask;
        }

        public async Task SendAndHandle(object msg, int times = 1)
        {
            await Send(msg);
            for (var i = 0; i < times; i++)
                (await TryHandleNext()).Should().BeTrue();
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask();
        }
    }
}
