using LinqToDB;
using LinqToDB.Data;

namespace PolyPersist.Net.AnalyticalStore.Postgres
{
    /// <summary>
    /// A single PostgreSQL fact table. Batch-first: <see cref="InsertBatch"/> bulk-copies rows
    /// (linq2db BulkCopy = PostgreSQL COPY); there is no per-row Insert/Update/Delete/Find (OLAP).
    /// <c>Query()</c> returns a composable linq2db IQueryable for in-database aggregation. The
    /// <c>class</c> constraint is required by linq2db; the store bridges to it via reflection.
    /// </summary>
    internal class Postgres_AnalyticalTable<TRecord> : IAnalyticalTable<TRecord>, IAnalyticalTableInternal
        where TRecord : class, IAnalyticalRecord, new()
    {
        private readonly string _name;
        private readonly Postgres_AnalyticalStore _store;

        // Public so the store can build it via Activator.CreateInstance across the class-constraint gap.
        public Postgres_AnalyticalTable(string name, Postgres_AnalyticalStore store)
        {
            _name = name;
            _store = store;
        }

        Task IAnalyticalTableInternal.CreateSchemaAsync() => _CreateSchemaAsync();

        private async Task _CreateSchemaAsync()
        {
            using var db = _store.CreateConnection();
            await db.CreateTableAsync<TRecord>(tableName: _name).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        string IAnalyticalTable<TRecord>.Name => _name;
        /// <inheritdoc/>
        IStore IAnalyticalTable<TRecord>.ParentStore => _store;

        /// <inheritdoc/>
        async Task IAnalyticalTable<TRecord>.InsertBatch(IList<TRecord> records)
        {
            if (records == null || records.Count == 0)
                return;

            using var db = _store.CreateConnection();
            await db.BulkCopyAsync(new BulkCopyOptions { TableName = _name }, records).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        System.Linq.IQueryable<TRecord> IAnalyticalTable<TRecord>.Query()
        {
            // A DataContext (not DataConnection) backs the returned IQueryable: it opens/closes the
            // ADO connection per query, so the queryable can be composed and enumerated after this
            // method returns without holding a connection open.
            var ctx = new DataContext(_store.Options());
            return ctx.GetTable<TRecord>().TableName(_name);
        }

        /// <inheritdoc/>
        object IAnalyticalTable<TRecord>.GetUnderlyingImplementation()
        {
            // A fresh linq2db DataConnection for raw SQL. The caller owns and disposes it.
            return _store.CreateConnection();
        }
    }
}
