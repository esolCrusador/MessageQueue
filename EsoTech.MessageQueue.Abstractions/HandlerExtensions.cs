using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EsoTech.MessageQueue.Abstractions
{
    public class HandlerExtensions
    {
        public static Func<object, CancellationToken, Task> CreateHandleDelegate(object instance, Type interfaceType, string methodName) =>
            CreateHandleDelegate(instance, interfaceType, interfaceType.GetMethod(methodName));
        public static Func<object, CancellationToken, Task> CreateHandleDelegate(object instance, Type interfaceType, MethodInfo methodInfo)
        {
            var messageType = interfaceType.GetGenericArguments()[0];
            var messageParameter = Expression.Parameter(typeof(object));
            var cancellationTokenParameter = Expression.Parameter(typeof(CancellationToken));
            var handlerInstance = Expression.Constant(instance);

            return Expression.Lambda<Func<object, CancellationToken, Task>>(
                Expression.Call(handlerInstance, methodInfo,
                    Expression.Convert(messageParameter, messageType),
                    cancellationTokenParameter
                ),
                messageParameter,
                cancellationTokenParameter
            ).Compile();
        }

        public static Func<object, TimeSpan?>? CreateGetTimeoutDelegate(object instance, Type messageType)
        {
            var interfaceType = typeof(ILongRunningHandler<>).MakeGenericType(messageType);
            if (!interfaceType.IsAssignableFrom(instance.GetType()))
                return null;

            var messageParameter = Expression.Parameter(typeof(object), "message");
            var method = interfaceType.GetMethod("GetTimeout");

            return Expression.Lambda<Func<object, TimeSpan?>>(Expression.Call(
                Expression.Convert(Expression.Constant(instance), interfaceType),
                method,
                Expression.Convert(messageParameter, messageType)
             ), messageParameter).Compile();
        }
    }
}
