using System.Collections;
using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Memory.Linq
{
    internal class Memory_Queryable<TResult> : IQueryable<TResult>
    {

        internal readonly IQueryable<TResult> _queryable;
        internal readonly Memory_QueryProvider _queryProvider;

        internal Memory_Queryable(IQueryable<TResult> queryable)
        { 
            _queryable = queryable;
            _queryProvider = new Memory_QueryProvider(_queryable.Provider);
        }

        public Type ElementType => _queryable.ElementType;

        public Expression Expression => _queryable.Expression;

        public IQueryProvider Provider => _queryProvider;

        public IEnumerator<TResult> GetEnumerator() => _queryable.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _queryable.GetEnumerator();
    }
}
