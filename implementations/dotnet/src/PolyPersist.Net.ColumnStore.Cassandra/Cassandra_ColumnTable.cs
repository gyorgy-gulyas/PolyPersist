using Cassandra;
using PolyPersist.Net.ColumnStore.Cassandra.Linq;
using PolyPersist.Net.Common;
using System.Collections.Concurrent;
using System.Linq;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal class Cassandra_ColumnTable<TRow> : IColumnTable<TRow>
        where TRow : IRow, new()
    {
        internal readonly ISession _session;
        internal readonly string _tableName;
        internal readonly TableMetadata _tableMeta;
        internal readonly Cassandra_ColumnStore _store;

        private static readonly ConcurrentDictionary<string, PreparedStatement> _preparedStatements = new();

        public Cassandra_ColumnTable(ISession session, string tableName, Cassandra_ColumnStore store)
        {
            _session = session;
            _tableName = tableName;
            _tableMeta = _session.Cluster.Metadata
                .GetKeyspace(_session.Keyspace)
                .GetTableMetadata(tableName);
            _store = store;
        }

        /// <inheritdoc/>
        string IColumnTable<TRow>.Name => _tableName;
        /// <inheritdoc/>
        IStore IColumnTable<TRow>.ParentStore => _store;

        /// <inheritdoc/>
        async Task IColumnTable<TRow>.Insert(TRow row)
        {
            CollectionCommon.CheckBeforeInsert(row);

            if (string.IsNullOrEmpty(row.id) == true)
                row.id = Guid.NewGuid().ToString();
            row.etag = Guid.NewGuid().ToString();
            row.LastUpdate = DateTime.UtcNow;

            var ps = await _getPreparedInsertStatement().ConfigureAwait(false);
            var accessors = MetadataHelper.GetAccessors<TRow>(lowerCaseNames: true);

            var values = new object[accessors.Count];
            int i = 0;
            foreach (var kvp in accessors)
                values[i++] = Cassandra_Mapper.MapToCassandra(kvp.Value.Getter(row));

            var bound = ps.Bind(values);

            var rs = await _session.ExecuteAsync(bound).ConfigureAwait(false);
            var applied = rs.FirstOrDefault()?.GetValue<bool>("[applied]") ?? false;

            if (applied == false )
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} cannot be inserted, because of duplicate key");
        }

        private PreparedStatement? _insertStatemant;
        private readonly SemaphoreSlim _insertStatemantLock = new(1, 1);
        private async Task<PreparedStatement> _getPreparedInsertStatement()
        {
            if (_insertStatemant == null)
            {
                await _insertStatemantLock.WaitAsync();

                try
                {
                    if (_insertStatemant == null) // re check!
                    {
                        var accessors = MetadataHelper.GetAccessors<TRow>(lowerCaseNames: true);
                        var columnNames = accessors.Keys;
                        var columnList = string.Join(", ", columnNames);
                        var placeholderList = string.Join(", ", columnNames.Select(_ => "?"));

                        _insertStatemant = await _PrepareOrGetAsync($"INSERT INTO {_session.Keyspace}.{_tableName} ({columnList}) VALUES ({placeholderList}) IF NOT EXISTS;").ConfigureAwait(false);
                    }
                }
                finally
                {
                    _insertStatemantLock.Release();
                }
            }

            return _insertStatemant!;
        }

        /// <inheritdoc/>
        async Task IColumnTable<TRow>.Update(TRow row)
        {
            CollectionCommon.CheckBeforeUpdate(row);

            var original_etag = row.etag;

            row.etag = Guid.NewGuid().ToString();
            row.LastUpdate = DateTime.UtcNow;

            var ps = await _getPreparedUpdateStatement().ConfigureAwait(false);
            var accessors = MetadataHelper.GetAccessors<TRow>(lowerCaseNames: true);

            var values = new object[accessors.Count + 1];
            int i = 0;
            foreach (var kvp in accessors)
            {
                if (kvp.Key == "id" || kvp.Key == "partitionkey" )
                    continue;
                values[i++] = Cassandra_Mapper.MapToCassandra(kvp.Value.Getter(row));
            }
            values[i++] = row.PartitionKey;
            values[i++] = row.id;
            values[i++] = original_etag;

            var bound = ps.Bind(values);
            var rs = await _session.ExecuteAsync(bound).ConfigureAwait(false);
            var applied = rs.FirstOrDefault()?.GetValue<bool>("[applied]") ?? false;

            if (applied == false)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} can not be updated, because it does not exist or was already changed");
        }

        private PreparedStatement? _updateStatemant;
        private readonly SemaphoreSlim _updateStatemantLock = new(1, 1);
        private async Task<PreparedStatement> _getPreparedUpdateStatement()
        {
            if (_updateStatemant == null)
            {
                await _updateStatemantLock.WaitAsync();

                try
                {
                    if (_updateStatemant == null) // re check!
                    {
                        var accessors = MetadataHelper.GetAccessors<TRow>(lowerCaseNames: true);

                        var dataFields = accessors
                            .Where(a => a.Key != "id" && a.Key != "partitionkey")
                            .ToList();

                        var assignments = string.Join(", ", dataFields.Select(a => a.Key + " = ?"));
                        _updateStatemant = await _PrepareOrGetAsync($"UPDATE {_session.Keyspace}.{_tableName} SET {assignments} WHERE partitionkey = ? AND id = ? IF etag = ?;").ConfigureAwait(false);
                    }
                }
                finally
                {
                    _updateStatemantLock.Release();
                }
            }

            return _updateStatemant!;
        }

        /// <inheritdoc/>
        async Task IColumnTable<TRow>.Delete(string partitionKey, string id)
        {
            // Single conditional statement (IF EXISTS) instead of SELECT-then-DELETE: atomic (no
            // TOCTOU) and one round-trip. 'applied' is false when the row was already gone.
            var ps = await _PrepareOrGetAsync($"DELETE FROM {_session.Keyspace}.{_tableName} WHERE partitionkey = ? AND id = ? IF EXISTS").ConfigureAwait(false);
            var rs = await _session.ExecuteAsync(ps.Bind(partitionKey, id)).ConfigureAwait(false);
            var applied = rs.FirstOrDefault()?.GetValue<bool>("[applied]") ?? false;
            if (applied == false)
                throw new Exception($"Row '{typeof(TRow).Name}' {id} can not be deleted because it is already removed.");
        }

        async Task<TRow> IColumnTable<TRow>.Find(string partitionKey, string id)
        {
            var ps = await _PrepareOrGetAsync($"SELECT * FROM {_session.Keyspace}.{_tableName} WHERE partitionkey = ? AND id = ? LIMIT 1;").ConfigureAwait(false);
            var bound = ps.Bind(partitionKey, id);
            var rs = await _session.ExecuteAsync(bound);

            var row = rs.FirstOrDefault();
            if (row == null)
                return default!;

            var mappedAccessors = await _getFindMappedAccessors(rs.Columns).ConfigureAwait(false);

            var result = new TRow();
            foreach (var (fieldIndex, memberAccessor, valueGetter) in mappedAccessors)
            {
                if (memberAccessor.Setter != null)
                {
                    var value = valueGetter(row, fieldIndex);
                    memberAccessor.Setter(result, value);
                }
            }

            return result;
        }

        private List<(int, MetadataHelper.MemberAccessor, Cassandra_Mapper.GetterDelegate)>? _findMappedAccessors;
        private readonly SemaphoreSlim _findMappedAccessorsLock = new(1, 1);

        private async Task<List<(int, MetadataHelper.MemberAccessor, Cassandra_Mapper.GetterDelegate)>> _getFindMappedAccessors(CqlColumn[] columns)
        {
            if (_findMappedAccessors == null)
            {
                await _findMappedAccessorsLock.WaitAsync();

                try
                {
                    if (_findMappedAccessors == null) // re check!
                    {

                        var accessors = MetadataHelper.GetAccessors<TRow>(lowerCaseNames: true);
                        var fieldIndexMap = new Dictionary<string, int>();
                        for (int i = 0; i < columns.Length; i++)
                            fieldIndexMap[columns[i].Name.ToLower()] = i;

                        _findMappedAccessors = accessors
                            .Where(kvp => fieldIndexMap.ContainsKey(kvp.Key))
                            .Select(kvp => (
                                FieldIndex: fieldIndexMap[kvp.Key],
                                MemberAccessor: kvp.Value,
                                CassandraValueGetter: Cassandra_Mapper.BuildTypedGetter(kvp.Value.Type)))
                            .ToList();
                    }
                }
                finally
                {
                    _findMappedAccessorsLock.Release();
                }
            }

            return _findMappedAccessors!;
        }

        /// <inheritdoc/>
        System.Linq.IQueryable<TRow> IColumnTable<TRow>.Query(string partitionKey)
            // A single-partition CQL query (WHERE partitionkey = ?): efficient, no ALLOW FILTERING.
            => new Cassandra_Queryable<TRow, TRow>(this).Where(row => row.PartitionKey == partitionKey);

        /// <inheritdoc/>
        System.Linq.IQueryable<TRow> IColumnTable<TRow>.QueryCrossPartition()
            => new Cassandra_Queryable<TRow, TRow>(this);

        /// <inheritdoc/>
        object IColumnTable<TRow>.GetUnderlyingImplementation() => _session;

        private async Task<PreparedStatement> _PrepareOrGetAsync(string cql)
        {
            if (_preparedStatements.TryGetValue(cql, out var ps))
                return ps;
            ps = await _session.PrepareAsync(cql);
            _preparedStatements[cql] = ps;
            return ps;
        }
    }
}