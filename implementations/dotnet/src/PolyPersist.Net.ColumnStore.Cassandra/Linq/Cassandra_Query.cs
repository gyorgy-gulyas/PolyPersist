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
        }

        internal string cql;
        internal ResultTypes resultType;
        internal Dictionary<string, string> projectionMemberMap;
        internal NewExpression projectionCtorForAnonymous;
    }
}
