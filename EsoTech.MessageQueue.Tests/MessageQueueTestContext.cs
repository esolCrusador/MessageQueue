using System.Diagnostics;
using System.Threading.Tasks;
using System;

namespace EsoTech.MessageQueue.Tests
{
    public class MessageQueueTestContext
    {
        public static async ValueTask Wait(Func<ValueTask<bool>> condition, TimeSpan? timeout = default)
        {
            timeout ??= Debugger.IsAttached ? TimeSpan.FromMinutes(1) : TimeSpan.FromSeconds(5);
            var endDate = DateTimeOffset.UtcNow + timeout;

            while (DateTimeOffset.UtcNow < endDate)
            {
                if (await condition())
                    return;

                await Task.Delay(timeout.Value / 50);
            }

            throw new TimeoutException($"Timeout {timeout} has passed");
        }

        public static ValueTask Wait(Func<bool> condition, TimeSpan? timeout = default)
        {
            return Wait(() => new ValueTask<bool>(condition()), timeout);
        }
    }
}
