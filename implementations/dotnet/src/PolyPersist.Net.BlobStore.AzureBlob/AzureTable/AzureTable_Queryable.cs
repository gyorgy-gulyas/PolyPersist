using Azure.Data.Tables;
using System.Collections;
using System.Linq.Expressions;

namespace PolyPersist.Net.BlobStore.AzureBlob.AzureTable
{
    internal class AzureTable_Queryable<T> : IQueryable<T>
    {
        private readonly TableClient _tableClient;

        public AzureTable_Queryable(TableClient tableClient)
        {
            _tableClient = tableClient;
            Provider = new AzureTable_QueryProvider<T>(_tableClient);
            Expression = Expression.Constant(this);
        }

        public Type ElementType => typeof(T);
        public Expression Expression { get; set; }
        public IQueryProvider Provider { get; }

        public IEnumerator<T> GetEnumerator() => Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
