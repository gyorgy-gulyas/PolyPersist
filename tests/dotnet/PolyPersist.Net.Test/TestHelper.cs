using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace PolyPersist.Net.Test
{
    [ExcludeFromCodeCoverage]
    public static class MethodBaseExtensions
    {
        public static string GetAsyncMethodName(this MethodBase method)
        {
            Type generatedType = method.DeclaringType;
            Type originalType = generatedType.DeclaringType;

            // not async method
            if (originalType == null)
                return method.Name;

            IEnumerable<MethodInfo> matchingMethods =
                from methodInfo in originalType.GetMethods()
                let attr = methodInfo.GetCustomAttribute<AsyncStateMachineAttribute>()
                where attr != null && attr.StateMachineType == generatedType
                select methodInfo;

            return matchingMethods.Single().Name;
        }
    }
}
