using System.Collections;
using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Cassandra.Linq
{
    internal class Cassandra_Queryable<TRow, TResult> : IOrderedQueryable<TResult>
       where TRow : IRow, new()
    {
        protected readonly Expression _expression;
        protected readonly Cassandra_QueryProvider<TRow> _provider;

        public Cassandra_Queryable(Cassandra_ColumnTable<TRow> table)
        {
            _provider = new Cassandra_QueryProvider<TRow>(table);
            _expression = Expression.Constant(this);
        }

        public Cassandra_Queryable(Cassandra_QueryProvider<TRow> provider, Expression expression)
        {
            _provider = provider;
            _expression = expression;
        }

        public Type ElementType => typeof(TResult);
        public Expression Expression => _expression;
        public IQueryProvider Provider => _provider;

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public IEnumerator<TResult> GetEnumerator()
        {
            var result = Provider.Execute(Expression);
            var enumerable = (IEnumerable<TResult>)result;

            return enumerable.GetEnumerator();
        }

        //IAsyncEnumerator<TRow> IAsyncEnumerable<TRow>.GetAsyncEnumerator(CancellationToken cancellationToken) => GetAsyncEnumerator(cancellationToken);
        //public async IAsyncEnumerator<TRow> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        //{
        //    await foreach (var item in _provider.ExecuteAsync(_expression).WithCancellation(cancellationToken))
        //    {
        //        yield return item;
        //    }
        //}
    }
}
