using Cassandra;
using PolyPersist.Net.Common;
using System.Collections.Concurrent;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal class Cassandra_ColumnTable<TRow> : IColumnTable<TRow>
        where TRow : IRow, new()
    {
        internal readonly ISession _session;
        internal readonly string _tableName;
        private static readonly ConcurrentDictionary<string, PreparedStatement> _preparedStatements = new();

        public Cassandra_ColumnTable(ISession session, string tableName)
        {
            _session = session;
            _tableName = tableName;
        }

        string IColumnTable<TRow>.Name => _tableName;

        async Task IColumnTable<TRow>.Insert(TRow row)
        {
            await CollectionCommon.CheckBeforeInsert(row).ConfigureAwait(false);
            row.etag = Guid.NewGuid().ToString();

            var ps = await _getPreparedInsertStatement().ConfigureAwait(false);
            var accessors = MetadataHelper.GetAccessors<TRow>(lowerCaseNames: true);

            var values = new object[accessors.Count];
            int i = 0;
            foreach (var kvp in accessors)
                values[i++] = kvp.Value.Getter(row);

            var bound = ps.Bind(values);
            await _session.ExecuteAsync(bound).ConfigureAwait(false);
        }

        static PreparedStatement _insertStatemant;
        private static readonly SemaphoreSlim _insertStatemantLock = new(1, 1);
        private async Task<PreparedStatement> _getPreparedInsertStatement()
        { 
            if(_insertStatemant == null )
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

                        _insertStatemant = await _PrepareOrGetAsync($"INSERT INTO {_tableName} ({columnList}) VALUES ({placeholderList});").ConfigureAwait(false);
                    }
                }
                finally
                {
                    _insertStatemantLock.Release();
                }
            }

            return _insertStatemant;
        }

        async Task IColumnTable<TRow>.Update(TRow row)
        {
            await CollectionCommon.CheckBeforeUpdate(row).ConfigureAwait(false);

            var original_etag = await _FindInternal(row.PartitionKey, row.id).ConfigureAwait(false);
            if (original_etag == null)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} can not be updated because it is already removed.");

            if (row.etag != original_etag)
                throw new Exception($"Document '{typeof(TRow).Name}' {row.id} can not be updated because it is already changed");

            row.etag = Guid.NewGuid().ToString();

            var ps = await _getPreparedInsertStatement().ConfigureAwait(false);
            var accessors = MetadataHelper.GetAccessors<TRow>(lowerCaseNames: true);

            var values = new object[accessors.Count];
            int i = 0;
            foreach (var kvp in accessors)
            {
                if (kvp.Key == "id" || kvp.Key == "partitionkey")
                    continue;
                values[i++] = kvp.Value.Getter(row);
            }
            values[i++] = row.PartitionKey;
            values[i++] = row.id;

            var bound = ps.Bind(values);
            await _session.ExecuteAsync(bound).ConfigureAwait(false);
        }

        static PreparedStatement _updateStatemant;
        private static readonly SemaphoreSlim _updateStatemantLock = new(1, 1);
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
                        _updateStatemant = await _PrepareOrGetAsync($"UPDATE {_tableName} SET {assignments} WHERE partitionkey = ? AND id = ?;").ConfigureAwait(false);
                    }
                }
                finally
                {
                    _updateStatemantLock.Release();
                }
            }

            return _updateStatemant;
        }

        async Task IColumnTable<TRow>.Delete(string partitionKey, string id)
        {
            var original_etag = await _FindInternal(partitionKey, id).ConfigureAwait(false);
            if (original_etag == null)
                throw new Exception($"Row '{typeof(TRow).Name}' {id} can not be deleted because it is already removed.");

            var ps = await _PrepareOrGetAsync($"DELETE FROM {_tableName} WHERE partitionkey = ? AND id = ?").ConfigureAwait(false);
            await _session.ExecuteAsync(ps.Bind(partitionKey, id));
        }

        async Task<TRow> IColumnTable<TRow>.Find(string partitionKey, string id)
        {
            var ps = await _PrepareOrGetAsync($"SELECT * FROM {_tableName} WHERE partitionkey = ? AND id = ? LIMIT 1;").ConfigureAwait(false);
            var rs = await _session.ExecuteAsync(ps.Bind(partitionKey, id));

            var row = rs.FirstOrDefault();
            if (row == null)
                return default;

            var mappedAccessors = await _getFindMappedAccessors(rs.Columns).ConfigureAwait( false );

            var result = new TRow();
            foreach (var (fieldIndex, memberAccessor, cassandraValueGetter) in mappedAccessors)
            {
                if (memberAccessor.Setter != null)
                {
                    var value = cassandraValueGetter(row, fieldIndex);
                    memberAccessor.Setter(result, value);
                }
            }

            return result;
        }

        static List<(int, MetadataHelper.MemberAccessor, Cassandra_ValueHelper.GetterDelegate)> _findMappedAccessors;
        private static readonly SemaphoreSlim _findMappedAccessorsLock = new(1, 1);

        private async Task<List<(int, MetadataHelper.MemberAccessor, Cassandra_ValueHelper.GetterDelegate)>> _getFindMappedAccessors( CqlColumn[] columns )
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
                                CassandraValueGetter: Cassandra_ValueHelper.BuildTypedGetter(kvp.Value.Type)))
                            .ToList();
                    }
                }
                finally
                {
                    _findMappedAccessors.Release();
                }
            }

            return _findMappedAccessors;
        }

        object IColumnTable<TRow>.Query()
        {
            return new Cassandra_Queryable<TRow>(this);
        }

        object IColumnTable<TRow>.GetUnderlyingImplementation() => _session;

        private async Task<string> _FindInternal(string partitionKey, string id)
        {
            var cql = $"SELECT * FROM {_tableName} WHERE partitionkey = ? AND id = ? LIMIT 1;";
            var ps = await _PrepareOrGetAsync(cql);
            var bound = ps.Bind(partitionKey, id);
            var rs = await _session.ExecuteAsync(bound).ConfigureAwait(false);

            var row = rs.FirstOrDefault();
            if (row == null)
                return default;

            return row.GetValue<string>("etag");
        }

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