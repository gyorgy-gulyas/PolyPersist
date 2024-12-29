using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.Memory
{
    internal class MemoryBlobStore_Collection<TEntity> : IBlobCollection<TEntity>
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

            IBlob blob = (IBlob)entity;
            _BlobData blobData = new()
            {
                id = entity.id,
                partionKey = entity.PartitionKey,
                etag = entity.etag,
                MetadataJSON = JsonSerializer.Serialize(entity, typeof(TEntity), JsonOptionsProvider.Options),
            };

            _collectionData.MapOfBlobs.Add((entity.id, entity.PartitionKey), blobData);
            _collectionData.ListOfBlobs.Add(blobData);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            if (_collectionData.MapOfBlobs.TryGetValue((entity.id, entity.PartitionKey), out _BlobData blobData) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be removed because it is does dot exist");

            if (blobData.etag != entity.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed");

            IBlob blob = (IBlob)entity;
            entity.etag = Guid.NewGuid().ToString();
            blobData.etag = entity.etag;
            blobData.MetadataJSON = JsonSerializer.Serialize(entity, typeof(TEntity), JsonOptionsProvider.Options);
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
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed because it is does not exist");

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
                return Task.FromResult(entity);
            }

            return Task.FromResult(default(TEntity));
        }

        /// <inheritdoc/>
        TQuery ICollection<TEntity>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TEntity>).IsAssignableFrom(typeof(TQuery));
            if (isQueryable == false)
                throw new Exception($"TQuery is must be 'IQueryable<TEntity>' in dotnet implementation");

            return (TQuery)_collectionData
            .ListOfBlobs
                .Select( b => JsonSerializer.Deserialize<TEntity>(b.MetadataJSON, JsonOptionsProvider.Options))
                .AsQueryable();
        }

        /// <inheritdoc/>s
        object ICollection<TEntity>.GetUnderlyingImplementation()
        {
            return _collectionData;
        }

        Task IBlobCollection<TEntity>.UploadContent(TEntity entity, Stream source)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((entity.id, entity.PartitionKey), out _BlobData blob) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not upload, because it is does not exist");

            blob.Content = _streamToByteArray(source);
            return Task.CompletedTask;
        }

        Task IBlobCollection<TEntity>.DownloadContentTo(TEntity entity, Stream destination)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((entity.id, entity.PartitionKey), out _BlobData blob) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not download, because it is does not exist");

            destination.Write(blob.Content, 0, blob.Content.Length);
            return Task.CompletedTask;
        }


        public static byte[] _streamToByteArray(Stream input)
        {
            using MemoryStream ms = new MemoryStream();
            input.CopyTo(ms);
            return ms.ToArray();
        }
    }
}
