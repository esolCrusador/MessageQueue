using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;

namespace EsoTech.MessageQueue.Extensions
{
    internal static class ExceptionExtensions
    {
        [DoesNotReturn] public static void Rethrow(this Exception ex) => ExceptionDispatchInfo.Capture(ex).Throw();
    }
}
