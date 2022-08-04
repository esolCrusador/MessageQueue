using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EsoTech.MessageQueue.Abstractions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddMessageHandler<T>(this IServiceCollection self) where T : class, IMessageHandler
        {
            self.AddSingleton<T>();
            self.AddSingleton<IMessageHandler>(ctx => ctx.GetRequiredService<T>());
            return self;
        }

        public static IServiceCollection TryAddMessageHandler<T>(this IServiceCollection self) where T : class, IMessageHandler
        {
            self.TryAddSingleton<T>();
            self.AddSingleton<IMessageHandler>(ctx => ctx.GetRequiredService<T>());
            return self;
        }

        public static IServiceCollection SuppressContinuousPolling(this IServiceCollection self)
            => self.AddSingleton<ContinuousPollingSuppressor>();
    }
}
