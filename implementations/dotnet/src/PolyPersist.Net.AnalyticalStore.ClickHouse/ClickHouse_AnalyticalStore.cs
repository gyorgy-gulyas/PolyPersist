using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;

namespace PolyPersist.Net.AnalyticalStore.ClickHouse
{
    /// <summary>
    /// Analytical (OLAP) store on ClickHouse, built on linq2db (ClickHouse.Client provider). This is
    /// the native columnar OLAP backend; the same implementation also serves ClickHouse Cloud (only
    /// the connection string differs). Writing is batch-first (<see cref="IAnalyticalTable{TRecord}"/>
    /// .InsertBatch via linq2db BulkCopy); the read side is LINQ (linq2db). Fact tables are created
    /// with the MergeTree engine.
    /// </summary>
    public class ClickHouse_AnalyticalStore : IAnalyticalStore
    {
        private const string _provider = ProviderName.ClickHouseClient;
        private readonly string _connectionString;

        internal readonly MappingSchema MappingSchema = new();

        /// <param name="connectionString">the ClickHouse.Client connection string.</param>
        public ClickHouse_AnalyticalStore(string connectionString)
        {
            _connectionString = connectionString;

            // linq2db's ClickHouse provider cannot render a bare decimal literal (it needs a
            // precision), so a decimal constant in a WHERE (e.g. Amount > 30m) throws. Emit decimals
            // as plain invariant numbers; ClickHouse coerces them to the column's Decimal type.
            MappingSchema.SetValueToSqlConverter(typeof(decimal),
                (sb, _, _, v) => sb.Append(((decimal)v).ToString(System.Globalization.CultureInfo.InvariantCulture)));
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Analytical;
        /// <inheritdoc/>
        string IStore.ProviderName => "Analytical(ClickHouse)";

        internal DataOptions Options()
            => new DataOptions().UseConnectionString(_provider, _connectionString).UseMappingSchema(MappingSchema);

        internal DataConnection CreateConnection() => new(Options());

        private bool _IsTableExists(string tableName)
        {
            using var db = CreateConnection();
            try
            {
                db.Execute($"SELECT 1 FROM \"{tableName}\" WHERE 1 = 0");
                return true;
            }
            catch
            {
                return false;
            }
        }

        // Bridges the class-constraint gap (linq2db requires `class`, the interface only guarantees
        // `IAnalyticalRecord, new()`); the table is built via reflection.
        private IAnalyticalTable<TRecord> _NewTable<TRecord>(string tableName) where TRecord : IAnalyticalRecord, new()
        {
            var type = typeof(ClickHouse_AnalyticalTable<>).MakeGenericType(typeof(TRecord));
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

            using var db = CreateConnection();
            await db.ExecuteAsync($"DROP TABLE \"{tableName}\"").ConfigureAwait(false);
        }
    }

    internal interface IAnalyticalTableInternal
    {
        Task CreateSchemaAsync();
    }
}
