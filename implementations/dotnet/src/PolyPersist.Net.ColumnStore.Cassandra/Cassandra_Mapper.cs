using Cassandra;
using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal static class Cassandra_Mapper
    {
        private static readonly Dictionary<Type, string> _typeMap = new()
        {
            [typeof(string)] = "text",
            [typeof(int)] = "int",
            [typeof(long)] = "bigint",
            [typeof(bool)] = "boolean",
            [typeof(double)] = "double",
            [typeof(Guid)] = "uuid",
            [typeof(DateTime)] = "timestamp",
            [typeof(decimal)] = "decimal",
            [typeof(DateOnly)] = "text",
            [typeof(TimeOnly)] = "text",
        };

        private static readonly ConcurrentDictionary<Type, Func<object, object>> _toCassandraConverters = new();

        internal static string MapType(Type type)
        {
            if (_typeMap.TryGetValue(type, out var cassandraType))
                return cassandraType;
            throw new NotSupportedException($"Type '{type.Name}' is not supported for Cassandra mapping.");
        }

        internal static object MapToCassandra(object value)
        {
            if (value == null) return null;

            var type = value.GetType();

            var converter = _toCassandraConverters.GetOrAdd(type, _CreateToConverter);
            return converter(value);
        }

        private static Func<object, object> _CreateToConverter(Type type)
        {
            if (type == typeof(DateOnly))
                return v => ((DateOnly)v).ToString("yyyy-MM-dd");

            if (type == typeof(TimeOnly))
                return v => ((TimeOnly)v).ToString("HH:mm:ss.fff");

            return v => v; // Default: no conversion
        }

        internal delegate object GetterDelegate(Row row, int index);

        internal static GetterDelegate BuildTypedGetter(Type targetType)
        {
            var underlyingType = Nullable.GetUnderlyingType(targetType);
            var actualType = underlyingType ?? targetType;

            Type readType = actualType;
            if (actualType == typeof(DateOnly) || actualType == typeof(TimeOnly))
                readType = typeof(string);

            var method = typeof(Row)
                .GetMethod("GetValue", new[] { typeof(int) })!
                .MakeGenericMethod(readType);

            var rowParam = Expression.Parameter(typeof(Row), "row");
            var indexParam = Expression.Parameter(typeof(int), "index");
            var callExpr = Expression.Call(rowParam, method, indexParam);

            // Automatikus konverzió a tényleges cél típusra (ha kell)
            Expression body = actualType switch
            {
                Type t when t == typeof(DateOnly) =>
                    Expression.Convert(
                        Expression.Call(typeof(DateOnly).GetMethod(nameof(DateOnly.ParseExact), new[] { typeof(string), typeof(string) })!,
                            callExpr,
                            Expression.Constant("yyyy-MM-dd")
                        ), typeof(object)
                    ),

                Type t when t == typeof(TimeOnly) =>
                    Expression.Convert(
                        Expression.Call(typeof(TimeOnly).GetMethod(nameof(TimeOnly.ParseExact), new[] { typeof(string), typeof(string) })!,
                            callExpr,
                            Expression.Constant("HH:mm:ss.fff")
                        ), typeof(object)
                    ),

                _ => Expression.Convert(callExpr, typeof(object))
            };

            var lambda = Expression.Lambda<GetterDelegate>(body, rowParam, indexParam);
            return lambda.Compile();
        }
    }
}
