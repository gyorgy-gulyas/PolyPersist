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
            await CollectionCommon.CheckBeforeInsert(entity).ConfigureAwait(false);

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
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            if (_collectionData.MapOfDocments.TryGetValue((entity.id, entity.PartitionKey), out _RowData row) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be removed because it is already removed");

            if (row.etag != entity.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed");

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
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed because it is already removed");

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

        /// <inheritdoc/>
        TQuery ICollection<TEntity>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TEntity>).IsAssignableFrom(typeof(TQuery));
            if (isQueryable == false)
                throw new Exception($"TQuery is must be 'IQueryable<TEntity>' in dotnet implementation");

            return (TQuery)_collectionData.ListOfDocments.AsQueryable();
        }

        /// <inheritdoc/>
        object ICollection<TEntity>.GetUnderlyingImplementation()
        {
            return _collectionData;
        }
    }
}
