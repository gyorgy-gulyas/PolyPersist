using System.Reflection;
using Google.Cloud.BigQuery.V2;

namespace PolyPersist.Net.AnalyticalStore.GCPBigQuery
{
    /// <summary>
    /// Analytical (OLAP) store on Google BigQuery. BigQuery has no linq2db provider, so this backend
    /// ships its own LINQ-to-GoogleSQL provider (see <see cref="BigQuery_QueryProvider"/>): the
    /// portable <c>Query()</c> surface translates LINQ (Where / GroupBy / aggregates / ...) into
    /// GoogleSQL that BigQuery executes server-side - the same result-based tests pass here as on the
    /// linq2db backends. Writing is batch-first (INSERT DML). Works against real BigQuery and the
    /// goccy/bigquery-emulator (the client is pre-built by the caller with the right endpoint).
    /// </summary>
    public class BigQuery_AnalyticalStore : IAnalyticalStore
    {
        internal BigQueryClient Client { get; }
        internal string Dataset { get; }

        /// <param name="client">a configured BigQuery client (real endpoint or emulator BaseUri).</param>
        /// <param name="datasetId">the dataset that holds the fact tables.</param>
        public BigQuery_AnalyticalStore(BigQueryClient client, string datasetId)
        {
            Client = client;
            Dataset = datasetId;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Analytical;
        /// <inheritdoc/>
        string IStore.ProviderName => "Analytical(BigQuery)";

        // Backtick-quoted, fully-qualified table reference for GoogleSQL.
        internal string TableRef(string tableName) => $"`{Dataset}.{tableName}`";

        private bool _IsTableExists(string tableName)
        {
            try
            {
                Client.ExecuteQuery($"SELECT 1 FROM {TableRef(tableName)} WHERE 1 = 0", parameters: null);
                return true;
            }
            catch
            {
                return false;
            }
        }

        // Bridges the class-constraint gap (the LINQ provider / materialization needs `class`, the
        // interface only guarantees `IAnalyticalRecord, new()`); the table is built via reflection.
        private IAnalyticalTable<TRecord> _NewTable<TRecord>(string tableName) where TRecord : IAnalyticalRecord, new()
        {
            var type = typeof(BigQuery_AnalyticalTable<>).MakeGenericType(typeof(TRecord));
            return (IAnalyticalTable<TRecord>)Activator.CreateInstance(type, tableName, this)!;
        }

        /// <inheritdoc/>
        Task<bool> IAnalyticalStore.IsTableExists(string tableName)
            => Task.FromResult(_IsTableExists(tableName));

        /// <inheritdoc/>
        Task<IAnalyticalTable<TRecord>> IAnalyticalStore.GetTableByName<TRecord>(string tableName)
        {
            if (_IsTableExists(tableName) == false)
                throw new Exception($"Table '{tableName}' does not exist in the analytical store");

            return Task.FromResult(_NewTable<TRecord>(tableName));
        }

        /// <inheritdoc/>
        async Task<IAnalyticalTable<TRecord>> IAnalyticalStore.CreateTable<TRecord>(string tableName)
        {
            if (_IsTableExists(tableName) == true)
                throw new Exception($"Table '{tableName}' already exists in the analytical store");

            var table = _NewTable<TRecord>(tableName);
            await ((IAnalyticalTableInternal)table).CreateSchemaAsync().ConfigureAwait(false);
            return table;
        }

        /// <inheritdoc/>
        async Task IAnalyticalStore.DropTable(string tableName)
        {
            if (_IsTableExists(tableName) == false)
                throw new Exception($"Table '{tableName}' does not exist in the analytical store");

            await Client.ExecuteQueryAsync($"DROP TABLE {TableRef(tableName)}", parameters: null).ConfigureAwait(false);
        }
    }

    internal interface IAnalyticalTableInternal
    {
        Task CreateSchemaAsync();
    }
}
