using EsoTech.MessageQueue.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using System;

namespace EsoTech.MessageQueue.Testing
{
    public static class ServiceCollectionExtensions
    {
        public static FakeMessageQueue AddFakeMessageQueue(this IServiceCollection services, FakeMessageQueue fakeMessageQueue = null, bool pullAutomatically = false)
        {
            fakeMessageQueue ??= new FakeMessageQueue();
            services.AddSingleton(s => fakeMessageQueue);
            services.AddSingleton<FakeMessageQueueInitializer>();
            services.AddSingleton<IMessageQueue>(s => s.GetRequiredService<FakeMessageQueue>());
            services.AddSingleton<IMessageConsumer>(s => s.GetRequiredService<FakeMessageQueue>());

            if (!pullAutomatically)
                services.SuppressContinuousPolling();

            return fakeMessageQueue;
        }

        public static void ImportFakeMessageQueue(this IServiceCollection services, IServiceProvider serviceProvider)
        {
            var fakeMessageQueue = serviceProvider.GetRequiredService<FakeMessageQueue>();
            services.AddSingleton(fakeMessageQueue);
            services.AddSingleton<IMessageQueue>(fakeMessageQueue);
            services.AddSingleton<IMessageConsumer>(fakeMessageQueue);
            services.AddSingleton(serviceProvider.GetRequiredService<FakeMessageQueueInitializer>());
        }

        public static void ImportFakeMessageQueue(this IServiceCollection services, Func<IServiceProvider, IServiceProvider> getServiceProvider)
        {
            services.AddSingleton(sp => getServiceProvider(sp).GetRequiredService<FakeMessageQueue>());
            services.AddSingleton<IMessageQueue>(sp => sp.GetRequiredService<FakeMessageQueue>());
            services.AddSingleton<IMessageConsumer>(sp => sp.GetRequiredService<FakeMessageQueue>());
            services.AddSingleton(sp => getServiceProvider(sp).GetRequiredService<FakeMessageQueueInitializer>());
        }
    }
}
