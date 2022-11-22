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
    }
}
