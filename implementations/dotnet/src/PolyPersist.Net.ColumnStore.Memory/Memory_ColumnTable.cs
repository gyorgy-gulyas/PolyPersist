using PolyPersist.Net.Common;
using PolyPersist.Net.ColumnStore.Memory.Linq;
using System.Text.Json;

namespace PolyPersist.Net.ColumnStore.Memory
{
    internal class Memory_ColumnTable<TRow> : IColumnTable<TRow>
        where TRow : IRow, new()
    {
        internal string _name;
        internal _TableData _tableData;
        internal Memory_ColumnStore _dataStore;

        internal Memory_ColumnTable(string name, _TableData tableData, Memory_ColumnStore dataStore)
        {
            _name = name;
            _tableData = tableData;
            _dataStore = dataStore;
        }

        /// <inheritdoc/>
        string IColumnTable<TRow>.Name => _name;
        /// <inheritdoc/>
        IStore IColumnTable<TRow>.ParentStore => _dataStore;

        /// <inheritdoc/>
        Task IColumnTable<TRow>.Insert(TRow row)
        {
            CollectionCommon.CheckBeforeInsert(row);

            row.etag = Guid.NewGuid().ToString();
            row.LastUpdate = DateTime.UtcNow;

            if (string.IsNullOrEmpty(row.id) == true)
                row.id = Guid.NewGuid().ToString();
            else if (_tableData.MapOfDocments.ContainsKey(row.id) == true)
                throw new DuplicateKeyException($"Row '{typeof(TRow).Name}' {row.id} cannot be inserted, beacuse of duplicate key");

            _RowData data = new()
            {
                id = row.id,
                partitionKey = row.PartitionKey,
                etag = row.etag,
                Value = JsonSerializer.Serialize(row, typeof(TRow), JsonOptionsProvider.Options())
            };

            _tableData.MapOfDocments.Add(row.id, data);
            _tableData.ListOfDocments.Add(data);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task IColumnTable<TRow>.Update(TRow row)
        {
            CollectionCommon.CheckBeforeUpdate(row);

            if (_tableData.MapOfDocments.TryGetValue(row.id, out _RowData? data) == false)
                throw new NotFoundException($"Row '{typeof(TRow).Name}' {row.id} can not be updated because does not exist");

            if (data.etag != row.etag)
                throw new ConcurrencyConflictException($"Row '{typeof(TRow).Name}' {row.id} can not be updated because it is already changed");

            row.etag = Guid.NewGuid().ToString();
            row.LastUpdate = DateTime.UtcNow;

            data.etag = row.etag;
            data.Value = JsonSerializer.Serialize(row, typeof(TRow), JsonOptionsProvider.Options());

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task IColumnTable<TRow>.Delete(string partitionKey, string id)
        {
            // only delete when the row exists in the requested partition
            if (_tableData.MapOfDocments.TryGetValue(id, out _RowData? row) == false || row.partitionKey != partitionKey)
                throw new NotFoundException($"Row '{typeof(TRow).Name}' {id} can not be removed because it is already removed");

            _tableData.MapOfDocments.Remove(id);
            _tableData.ListOfDocments.Remove(row);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TRow> IColumnTable<TRow>.Find(string partitionKey, string id)
        {
            // the row is identified by (partitionKey, id): a matching id in a different
            // partition is not the requested row.
            if (_tableData.MapOfDocments.TryGetValue(id, out _RowData? row) == true && row.partitionKey == partitionKey)
                return Task.FromResult(JsonSerializer.Deserialize<TRow>(row.Value, JsonOptionsProvider.Options())!);

            return Task.FromResult<TRow>(default!);
        }

        /// <inheritdoc/>
        System.Linq.IQueryable<TRow> IColumnTable<TRow>.Query(string partitionKey)
        {
            // Pre-filtered to one partition (in-memory) before the restricted Memory_Queryable
            // provider wraps it, so a caller's further .Where clauses still see the same operator
            // restrictions the Cassandra provider enforces.
            var queryable = _tableData
                .ListOfDocments
                .Select(data => JsonSerializer.Deserialize<TRow>(data.Value, JsonOptionsProvider.Options())!)
                .Where(row => row.PartitionKey == partitionKey)
                .AsQueryable();

            return new Memory_Queryable<TRow>(queryable);
        }

        /// <inheritdoc/>
        System.Linq.IQueryable<TRow> IColumnTable<TRow>.QueryCrossPartition()
        {
            var queryable = _tableData
                .ListOfDocments
                .Select(data => JsonSerializer.Deserialize<TRow>(data.Value, JsonOptionsProvider.Options())!)
                .AsQueryable();

            return new Memory_Queryable<TRow>(queryable);
        }

        /// <inheritdoc/>
        object IColumnTable<TRow>.GetUnderlyingImplementation()
        {
            return _tableData;
        }
    }
}
