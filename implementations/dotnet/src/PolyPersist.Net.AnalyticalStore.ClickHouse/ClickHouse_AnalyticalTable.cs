using System.Reflection;
using LinqToDB;
using LinqToDB.Data;

namespace PolyPersist.Net.AnalyticalStore.ClickHouse
{
    /// <summary>
    /// A single ClickHouse fact table (MergeTree engine). Batch-first: <see cref="InsertBatch"/>
    /// bulk-copies rows; there is no per-row Insert/Update/Delete/Find (OLAP). <c>Query()</c> returns
    /// a composable linq2db IQueryable for in-database aggregation. The <c>class</c> constraint is
    /// required by linq2db; the store bridges to it via reflection.
    /// </summary>
    internal class ClickHouse_AnalyticalTable<TRecord> : IAnalyticalTable<TRecord>, IAnalyticalTableInternal
        where TRecord : class, IAnalyticalRecord, new()
    {
        // Public read/write properties of the record -> columns (same names linq2db maps to).
        private static readonly PropertyInfo[] _props = typeof(TRecord)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .ToArray();

        private readonly string _name;
        private readonly ClickHouse_AnalyticalStore _store;

        public ClickHouse_AnalyticalTable(string name, ClickHouse_AnalyticalStore store)
        {
            _name = name;
            _store = store;
        }

        Task IAnalyticalTableInternal.CreateSchemaAsync() => _CreateSchemaAsync();

        private async Task _CreateSchemaAsync()
        {
            // linq2db's ClickHouse CreateTable always appends "ENGINE = Memory()" (transient, not
            // columnar). We build the DDL ourselves to use MergeTree - the standard persistent OLAP
            // engine; ORDER BY tuple() means "no sorting key", valid for an append-only fact table.
            // Column names match linq2db's mapping (property names), so BulkCopy and Query line up.
            string columns = string.Join(", ", _props.Select(p => $"\"{p.Name}\" {_ClickHouseType(p.PropertyType)}"));
            string sql = $"CREATE TABLE \"{_name}\" ({columns}) ENGINE = MergeTree() ORDER BY tuple()";

            using var db = _store.CreateConnection();
            await db.ExecuteAsync(sql).ConfigureAwait(false);
        }

        // Minimal .NET -> ClickHouse type map for fact columns. Value-type nullables become Nullable(T);
        // strings map to String (ClickHouse String is non-nullable by default, which suits dimensions).
        private static string _ClickHouseType(Type type)
        {
            Type? underlying = Nullable.GetUnderlyingType(type);
            Type t = underlying ?? type;

            string ch =
                t == typeof(string) ? "String" :
                t == typeof(int) ? "Int32" :
                t == typeof(long) ? "Int64" :
                t == typeof(short) ? "Int16" :
                t == typeof(byte) ? "UInt8" :
                t == typeof(bool) ? "Bool" :
                t == typeof(decimal) ? "Decimal(38, 9)" :
                t == typeof(double) ? "Float64" :
                t == typeof(float) ? "Float32" :
                t == typeof(DateTime) ? "DateTime64(3)" :
                t == typeof(Guid) ? "UUID" :
                "String";

            return underlying != null ? $"Nullable({ch})" : ch;
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
            var ctx = new DataContext(_store.Options());
            return ctx.GetTable<TRecord>().TableName(_name);
        }

        /// <inheritdoc/>
        object IAnalyticalTable<TRecord>.GetUnderlyingImplementation()
        {
            return _store.CreateConnection();
        }
    }
}
