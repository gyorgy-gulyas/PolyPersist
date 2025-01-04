using Azure.Data.Tables;
using System.Linq.Expressions;

namespace PolyPersist.Net.BlobStore.AzureBlob.AzureTable
{
    internal class AzureTable_QueryProvider<T> : IQueryProvider where T : class, ITableEntity, new()
    {
        private readonly TableClient _tableClient;

        public AzureTable_QueryProvider(TableClient tableClient)
        {
            _tableClient = tableClient;
        }

        public IQueryable CreateQuery(Expression expression)
        {
            return new AzureTable_Queryable<T>(_tableClient) { Expression = expression };
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return (IQueryable<TElement>)new AzureTable_Queryable<T>(_tableClient);
        }

        public object Execute(Expression expression)
        {
            return Execute<IEnumerable<T>>(expression);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            // OData szűrő létrehozása a LINQ kifejezésből
            var filter = AzureTable_ExpressionVisitor.GetODataFilter(expression);

            // Táblák lekérdezése az OData szűrő alapján
            var result = _tableClient.Query<T>(filter);

            return (TResult)(object)result;
        }
    }
}
