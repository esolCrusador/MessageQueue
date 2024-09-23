using EsoTech.MessageQueue.Abstractions.Aggregations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

namespace EsoTech.MessageQueue.Abstractions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddEventMessageHandler<TEventHandler>(this IServiceCollection self)
            where TEventHandler : class, IEventMessageHandler
        {
            self.AddSingleton<TEventHandler>();
            self.AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<TEventHandler>());
            return self;
        }

        public static IServiceCollection TryAddEventMessageHandler<TEventHandler>(this IServiceCollection self)
            where TEventHandler : class, IEventMessageHandler
        {
            self.TryAddSingleton<TEventHandler>();
            self.AddSingleton<IEventMessageHandler>(ctx => ctx.GetRequiredService<TEventHandler>());
            return self;
        }

        public static IServiceCollection AddCommandMessageHandler<TCommandHandler>(this IServiceCollection self)
            where TCommandHandler : class, ICommandMessageHandler
        {
            self.AddSingleton<TCommandHandler>();
            self.AddSingleton<ICommandMessageHandler>(ctx => ctx.GetRequiredService<TCommandHandler>());
            return self;
        }

        public static IServiceCollection TryAddCommandMessageHandler<TCommandHandler>(this IServiceCollection self)
            where TCommandHandler : class, ICommandMessageHandler
        {
            self.TryAddSingleton<TCommandHandler>();
            self.AddSingleton<ICommandMessageHandler>(ctx => ctx.GetRequiredService<TCommandHandler>());
            return self;
        }

        public static IServiceCollection AddMessageQueueSession(this IServiceCollection self)
        {
            self.AddScoped<IMessageQueueSession, MessageQueueSession>();
            self.AddSingleton<CombinedMessagesAggregator>();
            return self;
        }
        public static IServiceCollection AddMessageAggregator<TMessageAggregator>(this IServiceCollection services)
            where TMessageAggregator : class, IMessagesAggregator
        {
            services.AddSingleton<IMessagesAggregator, TMessageAggregator>();
            return services;
        }
        public static IServiceCollection AddMessageAggregator<TMessage, TAggregatedMessage>(this IServiceCollection services, Action<TAggregatedMessage, TMessage> addMessage)
            where TAggregatedMessage : class, new()
        {
            services.AddSingleton<IMessagesAggregator>(new SimpleMessagesAggregator<TMessage, TAggregatedMessage>(addMessage));
            return services;
        }

        public static IServiceCollection SuppressContinuousPolling(this IServiceCollection self)
            => self.Configure<MessageQueueConfiguration>(opts => opts.HandleRealtime = false);
    }
}
