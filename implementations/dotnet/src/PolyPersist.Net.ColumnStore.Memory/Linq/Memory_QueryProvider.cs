using System.Linq.Expressions;

namespace PolyPersist.Net.ColumnStore.Memory.Linq
{
    internal class Memory_QueryProvider : IQueryProvider
    {
        internal readonly IQueryProvider _queryProvider;

        internal Memory_QueryProvider(IQueryProvider queryProvider)
        {
            _queryProvider = queryProvider;
        }

        public IQueryable CreateQuery(Expression expression)
        {
            _checkExpressionCompatibility(expression);
            return _queryProvider.CreateQuery(expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            _checkExpressionCompatibility(expression);
            return _queryProvider.CreateQuery<TElement>(expression);
        }

        public object Execute(Expression expression)
        {
            _checkExpressionCompatibility(expression);
            return _queryProvider.Execute(expression);
        }

        private static void _checkExpressionCompatibility(Expression expression)
        {
            var visitor = new Memory_ExpressionVisitor();
            visitor.Visit(expression);
        }

        public TResult Execute<TResult>(Expression expression) => (TResult)Execute(expression);
    }
}
