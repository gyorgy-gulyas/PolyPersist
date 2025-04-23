using Cassandra;
using Cassandra.Mapping;
using PolyPersist.Net.Common;
using PolyPersist.Net.Core;
using System.Collections.Concurrent;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal class Cassandra_ColumnTable<TRow> : IColumnTable<TRow> 
        where TRow : IRow, new()
    {
        private readonly ISession _session;
        private readonly string _tableName;
        private readonly IMapper _mapper;

        public Cassandra_ColumnTable(ISession session, string tableName)
        {
            _session = session;
            _tableName = tableName;
            _mapper = new Mapper(session);
        }

        string IColumnTable<TRow>.Name => _tableName;

        async Task IColumnTable<TRow>.Insert(TRow row)
        {
            await CollectionCommon.CheckBeforeInsert(row).ConfigureAwait(false);
            row.etag = Guid.NewGuid().ToString();

            await _mapper.InsertAsync(row).ConfigureAwait(false);
        }

        async Task IColumnTable<TRow>.Update(TRow row)
        {
            await CollectionCommon.CheckBeforeUpdate(row).ConfigureAwait(false);

            var original = await _FindInternal(row.PartitionKey, row.id).ConfigureAwait(false);
            if (original == null)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} can not be updated because it is already removed.");

            if (row.etag != original.etag)
                throw new Exception($"Document '{typeof(TRow).Name}' {row.id} can not be updated because it is already changed");

            row.etag = Guid.NewGuid().ToString();
            await _mapper.UpdateAsync(row).ConfigureAwait(false);
        }

        async Task IColumnTable<TRow>.Delete(string partitionKey, string id)
        {
            var original = await _FindInternal(partitionKey, id).ConfigureAwait(false);
            if (original == null)
                throw new Exception($"Row '{typeof(TRow).Name}' {id} can not be deleted because it is already removed.");

            var query = $"DELETE FROM {_tableName} WHERE partitionkey = ? AND id = ?";
            await _session.ExecuteAsync(new SimpleStatement(query, partitionKey, id)).ConfigureAwait(false);
        }

        async Task<TRow> IColumnTable<TRow>.Find(string partitionKey, string id)
        {
            var query = $"SELECT * FROM {_tableName} WHERE partitionkey = ? AND id = ? LIMIT 1";
            var result = await _mapper.FetchAsync<TRow>(query, partitionKey, id).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        object IColumnTable<TRow>.GetUnderlyingImplementation() => _session;

        private async Task<Entity> _FindInternal(string partitionKey, string id)
        {
            var stmt = new SimpleStatement(
                $"SELECT id, etag FROM {_tableName} WHERE partitionkey = ? AND id = ?",
                partitionKey,
                id
            );

            var result = await _session.ExecuteAsync(stmt).ConfigureAwait(false);
            var row = result.FirstOrDefault();

            if (row == null)
                return default;

            return new Entity
            {
                PartitionKey = row.GetValue<string>("partitionkey"),
                id = row.GetValue<string>("id"),
                etag = row.GetValue<string>("etag"),
            };
        }
    }
}