using System.Linq;
using System.Reflection;
using Google.Cloud.BigQuery.V2;

namespace PolyPersist.Net.AnalyticalStore.GCPBigQuery
{
    /// <summary>
    /// A single BigQuery fact table. Batch-first: <see cref="InsertBatch"/> uses INSERT DML (chunked,
    /// parameterized) so rows are immediately queryable (unlike streaming inserts). <c>Query()</c>
    /// returns a LINQ-to-GoogleSQL IQueryable that pushes filtering/aggregation down to BigQuery.
    /// </summary>
    internal class BigQuery_AnalyticalTable<TRecord> : IAnalyticalTable<TRecord>, IAnalyticalTableInternal
        where TRecord : class, IAnalyticalRecord, new()
    {
        // Public read/write properties -> columns (names used verbatim in DDL, INSERT and SELECT).
        private static readonly PropertyInfo[] _props = typeof(TRecord)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .ToArray();

        private const int _InsertChunk = 500;

        private readonly string _name;
        private readonly BigQuery_AnalyticalStore _store;

        public BigQuery_AnalyticalTable(string name, BigQuery_AnalyticalStore store)
        {
            _name = name;
            _store = store;
        }

        Task IAnalyticalTableInternal.CreateSchemaAsync() => _CreateSchemaAsync();

        private async Task _CreateSchemaAsync()
        {
            // Backtick-quote column names: a property may collide with a GoogleSQL reserved word (e.g. "At").
            string columns = string.Join(", ", _props.Select(p => $"`{p.Name}` {_GoogleSqlType(p.PropertyType)}"));
            string sql = $"CREATE TABLE {_store.TableRef(_name)} ({columns})";
            await _store.Client.ExecuteQueryAsync(sql, parameters: null).ConfigureAwait(false);
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

            // Ingestion path = chunked INSERT DML with typed GoogleSQL literals (not query
            // parameters): literals are unambiguous and the BigQuery emulator mishandles typed
            // parameters (e.g. NUMERIC arrives as STRING). This is correct and immediately queryable
            // at moderate scale. PERFORMANCE NOTE: for high-volume ingestion, INSERT DML is NOT the
            // fast path (BigQuery imposes per-table DML quotas) - production should switch to a load
            // job (BigQueryClient.UploadJson) or the Storage Write API. Those paths cannot be used
            // here because the goccy emulator does not serve the upload endpoint (returns 0.0.0.0),
            // so they would be untestable; the switch is a production-only optimization.
            string cols = string.Join(", ", _props.Select(p => $"`{p.Name}`"));

            for (int start = 0; start < records.Count; start += _InsertChunk)
            {
                var chunk = records.Skip(start).Take(_InsertChunk).ToList();
                var rowSql = chunk.Select(rec =>
                    "(" + string.Join(", ", _props.Select(p => _Literal(p.PropertyType, p.GetValue(rec)))) + ")");

                string sql = $"INSERT INTO {_store.TableRef(_name)} ({cols}) VALUES {string.Join(", ", rowSql)}";
                await _store.Client.ExecuteQueryAsync(sql, parameters: null).ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        System.Linq.IQueryable<TRecord> IAnalyticalTable<TRecord>.Query(string partitionKey)
            => _CrossPartitionQuery().Where(r => r.PartitionKey == partitionKey);

        /// <inheritdoc/>
        System.Linq.IQueryable<TRecord> IAnalyticalTable<TRecord>.QueryCrossPartition()
            => _CrossPartitionQuery();

        private System.Linq.IQueryable<TRecord> _CrossPartitionQuery()
            => new BigQuery_Queryable<TRecord>(new BigQuery_QueryProvider(_store.Client, _store.TableRef(_name), _props));

        /// <inheritdoc/>
        object IAnalyticalTable<TRecord>.GetUnderlyingImplementation() => _store.Client;

        // ---- .NET -> GoogleSQL type maps ----

        private static string _GoogleSqlType(Type type)
        {
            Type t = Nullable.GetUnderlyingType(type) ?? type;
            return
                t == typeof(string) ? "STRING" :
                t == typeof(int) || t == typeof(long) || t == typeof(short) ? "INT64" :
                t == typeof(bool) ? "BOOL" :
                t == typeof(decimal) ? "NUMERIC" :
                t == typeof(double) || t == typeof(float) ? "FLOAT64" :
                t == typeof(DateTime) ? "DATETIME" :
                t == typeof(Guid) ? "STRING" :
                "STRING";
        }

        private static string _Literal(Type type, object? value)
        {
            if (value == null)
                return "NULL";

            Type t = Nullable.GetUnderlyingType(type) ?? type;
            var inv = System.Globalization.CultureInfo.InvariantCulture;

            if (t == typeof(string)) return "'" + _Escape((string)value) + "'";
            if (t == typeof(int) || t == typeof(long) || t == typeof(short) || t == typeof(byte))
                return Convert.ToInt64(value).ToString(inv);
            if (t == typeof(bool)) return (bool)value ? "true" : "false";
            if (t == typeof(decimal)) return "NUMERIC '" + ((decimal)value).ToString(inv) + "'";
            if (t == typeof(double) || t == typeof(float)) return Convert.ToDouble(value).ToString("R", inv);
            if (t == typeof(DateTime)) return "DATETIME '" + ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss.ffffff", inv) + "'";
            if (t == typeof(Guid)) return "'" + value + "'";

            return "'" + _Escape(value.ToString()!) + "'";
        }

        // GoogleSQL string-literal escaping (backslash-style).
        private static string _Escape(string s) => s.Replace("\\", "\\\\").Replace("'", "\\'");
    }
}
