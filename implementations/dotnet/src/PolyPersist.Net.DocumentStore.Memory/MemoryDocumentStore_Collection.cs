using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.DocumentStore.Memory
{
    internal class MemoryDocumentDB_Collection<TEntity> : ICollection<TEntity>
        where TEntity : IEntity, new()
    {
        internal string _name;
        internal _CollectionData _collectionData;
        internal MemoryDocumentDB_DataStore _dataStore;

        internal MemoryDocumentDB_Collection(string name, _CollectionData collectionData, MemoryDocumentDB_DataStore dataStore)
        {
            _name = name;
            _collectionData = collectionData;
            _dataStore = dataStore;
        }

        /// <inheritdoc/>
        string ICollection<TEntity>.Name => _name;

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Insert(TEntity entity)
        {
            if (entity is IValidable validable)
                await Validator.Validate(validable).ConfigureAwait(false);

            if (string.IsNullOrEmpty(entity.etag) == false)
                throw new Exception($"ETag is already filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.PartitionKey) == true)
                throw new Exception($"PartionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            entity.etag = Guid.NewGuid().ToString();

            if (string.IsNullOrEmpty(entity.id) == true)
                entity.id = Guid.NewGuid().ToString();

            _RowData row = new()
            {
                id = entity.id,
                partionKey = entity.PartitionKey,
                etag = entity.etag,
                Value = JsonSerializer.Serialize(entity, typeof(TEntity), JsonOptionsProvider.Options)
            };

            _collectionData.MapOfDocments.Add((entity.id, entity.PartitionKey), row);
            _collectionData.ListOfDocments.Add(row);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            if (entity is IValidable validable)
                await Validator.Validate(validable).ConfigureAwait(false);

            if (string.IsNullOrEmpty(entity.etag) == true)
                throw new Exception($"ETag must be filled at Update operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (_collectionData.MapOfDocments.TryGetValue((entity.id, entity.PartitionKey), out _RowData row) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be removed because it is already removed or changed.");

            if (row.etag != entity.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed or removed.");

            entity.etag = Guid.NewGuid().ToString();
            row.etag = entity.etag;
            row.Value = JsonSerializer.Serialize(entity, typeof(TEntity), JsonOptionsProvider.Options);
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(TEntity entity)
        {
            return (this as ICollection<TEntity>).Delete(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(string id, string partitionKey)
        {
            if (_collectionData.MapOfDocments.TryGetValue((id, partitionKey), out _RowData row) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed because it is already removed or changed.");

            _collectionData.MapOfDocments.Remove((id, partitionKey));
            _collectionData.ListOfDocments.Remove(row);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TEntity> ICollection<TEntity>.Find(string id, string partitionKey)
        {
            if (_collectionData.MapOfDocments.TryGetValue((id, partitionKey), out _RowData row) == true)
                return Task.FromResult(JsonSerializer.Deserialize<TEntity>(row.Value, JsonOptionsProvider.Options));

            return Task.FromResult(default(TEntity));
        }
    }
}
