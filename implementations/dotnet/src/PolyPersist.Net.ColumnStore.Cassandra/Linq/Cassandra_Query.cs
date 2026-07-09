using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Cassandra.Linq
{
    internal class Cassandra_Query
    {
        internal enum ResultTypes
        {
            Count,
            SelectRow,
            ProjectionToClass,
            ProjectionToAnonymous,
            ProjectionToSingleColumn,
        }

        internal string cql = null!;
        internal object[] parameters = System.Array.Empty<object>();   // bound values for '?' in cql
        internal ResultTypes resultType;
        internal Dictionary<string, string> projectionMemberMap = null!;
        internal NewExpression? projectionCtorForAnonymous;
    }
}
