using System.Diagnostics;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

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

    [ExcludeFromCodeCoverage]
    public static class StringExtensions
    {
        public const int MaxStorageNameLength = 63;
        [DebuggerStepThrough]
        public static string MakeStorageConformName(this string str)
        {
            StringBuilder sb = new StringBuilder();
            string lower = str.ToLower();
            foreach (char c in lower)
            {
                if (c.IsStorageConform() == true)
                {
                    sb.Append(c);
                }
            }

            string result = sb.ToString();

            while (result.Contains("--") == true)
                result = result.Replace("--", "-");

            if (result.Length > MaxStorageNameLength)
                result = result.Substring(0, MaxStorageNameLength);

            return result;
        }

        public static bool IsStorageConform(this Char c)
        {
            if (c == '-')
                return true;

            if ((c >= '0' && c <= '9'))
                return true;

            if ((c >= 'a' && c <= 'z'))
                return true;

            return false;
        }
    }

    [ExcludeFromCodeCoverage]
    public static class TestHelper
    {
        public static DateTime RoundToMilliseconds(DateTime dt)
        {
            return new DateTime(dt.Ticks - (dt.Ticks % TimeSpan.TicksPerMillisecond), DateTimeKind.Utc);
        }
    }
}
