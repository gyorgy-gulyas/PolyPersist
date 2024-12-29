using Azure.Storage.Blobs;
using PolyPersist.Net.Common;
using System.Text;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.AzureBlob
{
    internal class AzureBlob_Collection<TEntity> : ICollection<TEntity>
        where TEntity : IEntity, new()
    {
        private BlobContainerClient _containerClient;
        private Dictionary<string,TEntity> _entities = [];

        public AzureBlob_Collection(BlobContainerClient containerClient)
        {
            _containerClient = containerClient;

            BlobClient blobClient = _containerClient.GetBlobClient("__entities.json");
            if (blobClient.Exists() == true)
                _readEntities(blobClient);
            else
                _writeEntities().Wait();
        }

        /// <inheritdoc/>
        string ICollection<TEntity>.Name => _containerClient.Name;

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Insert(TEntity entity)
        {
            await CollectionCommon.CheckBeforeInsert(entity).ConfigureAwait(false);

            // create blob client
            IFile file = (IFile)entity;
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(file));

            // new etag
            file.etag = Guid.NewGuid().ToString();

            // set metadata
            await blobClient.SetMetadataAsync(_getMetadata(file)).ConfigureAwait(false);
            // upload content
            await blobClient.UploadAsync(file.content, overwrite: false).ConfigureAwait(false);

            _entities[_makePath(file)] = entity;
            await _writeEntities().ConfigureAwait( false );
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            // create blob client
            IFile file = (IFile)entity;
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(file));

            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it does not exist.");

            var properties = await blobClient.GetPropertiesAsync().ConfigureAwait(false);
            if (properties.Value.Metadata[nameof(file.etag)] != file.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed.");

            // new etag
            file.etag = Guid.NewGuid().ToString();

            // overwrite metadata
            await blobClient.SetMetadataAsync(_getMetadata(file)).ConfigureAwait(false);
            // upload content with overwrite
            await blobClient.UploadAsync(file.content, overwrite: true).ConfigureAwait(false);

            _entities[_makePath(file)] = entity;
            await _writeEntities().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(TEntity entity)
        {
            return (this as ICollection<TEntity>).Delete(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Delete(string id, string partitionKey)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(partitionKey, id));

            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be delete because it does not exist.");

            await blobClient.DeleteAsync().ConfigureAwait(false);

            _entities.Remove(_makePath(partitionKey, id));
            await _writeEntities().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        Task<TEntity> ICollection<TEntity>.Find(string id, string partitionKey)
        {
            if( _entities.TryGetValue(_makePath(partitionKey, id), out TEntity entity ) == false )
                return default;

            return Task.FromResult( entity );
        }

        /// <inheritdoc/>
        TQuery ICollection<TEntity>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TEntity>).IsAssignableFrom(typeof(TQuery));
            if (isQueryable == false)
                throw new Exception($"TQuery is must be 'IQueryable<TEntity>' in dotnet implementation");

            return (TQuery)_entities.Values.AsQueryable();
        }

        /// <inheritdoc/>
        object ICollection<TEntity>.GetUnderlyingImplementation()
        {
            return _containerClient;
        }

        private string _makePath(IFile file)
            => _makePath(file.PartitionKey, file.id);

        private string _makePath(string partitionKey, string id)
            => $"{partitionKey}/{id}";

        private Dictionary<string, string> _getMetadata(IFile file)
        {
            string json = JsonSerializer.Serialize(file, typeof(TEntity), JsonOptionsProvider.Options);
            string content = Convert.ToBase64String(Encoding.UTF8.GetBytes(json));

            return new Dictionary<string, string>() {
                { nameof(file.id), file.id },
                { nameof(file.PartitionKey), file.PartitionKey },
                { nameof(file.etag), file.etag },
                { nameof(file.fileName), file.fileName },
                { nameof(file.contentType), file.contentType },
                { "content", content }
            };
        }

        void _readEntities(BlobClient blobClient)
        {
            using var memoryStream = new MemoryStream();
            blobClient.DownloadTo(memoryStream);

            memoryStream.Position = 0;
            using var streamReader = new StreamReader(memoryStream, Encoding.UTF8);
            var json = streamReader.ReadToEnd();
            _entities = JsonSerializer.Deserialize<Dictionary<string, TEntity>>(json, JsonOptionsProvider.Options);
        }

        async Task _writeEntities()
        {
            BlobClient blobClient = _containerClient.GetBlobClient("__entities.json");

            string json = JsonSerializer.Serialize(_entities);
            using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            await blobClient.UploadAsync(memoryStream, overwrite: true).ConfigureAwait( false );
        }

    }
}
