using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.Memory
{
    internal class MemoryBlobStore_Collection<TEntity> : ICollection<TEntity>
        where TEntity : IEntity, new()
    {
        internal string _name;
        internal _CollectionData _collectionData;
        internal MemoryBlobStore_DataStore _dataStore;

        internal MemoryBlobStore_Collection(string name, _CollectionData collectionData, MemoryBlobStore_DataStore dataStore)
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

            IFile file = (IFile)entity;
            _BlobData blob = new()
            {
                id = entity.id,
                partionKey = entity.PartitionKey,
                etag = entity.etag,
                MetadataJSON = JsonSerializer.Serialize(entity, typeof(TEntity), JsonOptionsProvider.Options),
                Content = _streamToByteArray(file.content),
            };

            _collectionData.MapOfBlobs.Add((entity.id, entity.PartitionKey), blob);
            _collectionData.ListOfBlobs.Add(blob);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            if (_collectionData.MapOfBlobs.TryGetValue((entity.id, entity.PartitionKey), out _BlobData blob) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be removed because it is already removed");

            if (blob.etag != entity.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed");

            IFile file = (IFile)entity;
            entity.etag = Guid.NewGuid().ToString();
            blob.etag = entity.etag;
            blob.MetadataJSON = JsonSerializer.Serialize(entity, typeof(TEntity), JsonOptionsProvider.Options);
            blob.Content = _streamToByteArray(file.content);
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(TEntity entity)
        {
            return (this as ICollection<TEntity>).Delete(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(string id, string partitionKey)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((id, partitionKey), out _BlobData blob) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed because it is already removed");

            _collectionData.MapOfBlobs.Remove((id, partitionKey));
            _collectionData.ListOfBlobs.Remove(blob);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TEntity> ICollection<TEntity>.Find(string id, string partitionKey)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((id, partitionKey), out _BlobData blob) == true)
            {
                TEntity entity = JsonSerializer.Deserialize<TEntity>(blob.MetadataJSON, JsonOptionsProvider.Options);
                IFile file = (IFile)entity;
                file.content = _byteArrayToStream(blob.Content);

                return Task.FromResult(entity);
            }

            return Task.FromResult(default(TEntity));
        }

        public static byte[] _streamToByteArray(Stream input)
        {
            using MemoryStream ms = new MemoryStream();
            input.CopyTo(ms);
            return ms.ToArray();
        }

        public static Stream _byteArrayToStream(byte[] byteArray)
        {
            return new MemoryStream(byteArray);
        }
    }
}
