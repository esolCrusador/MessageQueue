using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

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

        public static IServiceCollection SuppressContinuousPolling(this IServiceCollection self)
            => self.AddSingleton<ContinuousPollingSuppressor>();
    }
}
