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
        async Task IColumnTable<TRow>.Insert(TRow row)
        {
            await CollectionCommon.CheckBeforeInsert(row).ConfigureAwait(false);

            row.etag = Guid.NewGuid().ToString();
            row.LastUpdate = DateTime.UtcNow;

            if (string.IsNullOrEmpty(row.id) == true)
                row.id = Guid.NewGuid().ToString();
            else if (_tableData.MapOfDocments.ContainsKey(row.id) == true)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} cannot be inserted, beacuse of duplicate key");

            _RowData data = new()
            {
                id = row.id,
                partitionKey = row.PartitionKey,
                etag = row.etag,
                Value = JsonSerializer.Serialize(row, typeof(TRow), JsonOptionsProvider.Options)
            };

            _tableData.MapOfDocments.Add(row.id, data);
            _tableData.ListOfDocments.Add(data);
        }

        /// <inheritdoc/>
        async Task IColumnTable<TRow>.Update(TRow row)
        {
            await CollectionCommon.CheckBeforeUpdate(row).ConfigureAwait(false);

            if (_tableData.MapOfDocments.TryGetValue(row.id, out _RowData data) == false)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} can not be updated because does not exist");

            if (data.etag != row.etag)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} can not be updated because it is already changed");

            row.etag = Guid.NewGuid().ToString();
            row.LastUpdate = DateTime.Now;

            data.etag = row.etag;
            data.Value = JsonSerializer.Serialize(row, typeof(TRow), JsonOptionsProvider.Options);
        }

        /// <inheritdoc/>
        Task IColumnTable<TRow>.Delete(string partitionKey, string id)
        {
            if (_tableData.MapOfDocments.TryGetValue(id, out _RowData row) == false)
                throw new Exception($"Row '{typeof(TRow).Name}' {id} can not be removed because it is already removed");

            _tableData.MapOfDocments.Remove(id);
            _tableData.ListOfDocments.Remove(row);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TRow> IColumnTable<TRow>.Find(string partitionKey, string id)
        {
            if (_tableData.MapOfDocments.TryGetValue(id, out _RowData row) == true)
                return Task.FromResult(JsonSerializer.Deserialize<TRow>(row.Value, JsonOptionsProvider.Options));

            return Task.FromResult<TRow>((TRow)default(TRow));
        }

        /// <inheritdoc/>
        object IColumnTable<TRow>.Query()
        {
            var queryable = _tableData
                .ListOfDocments
                .Select(data => JsonSerializer.Deserialize<TRow>(data.Value, JsonOptionsProvider.Options))
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
