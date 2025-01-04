using Azure;
using Azure.Data.Tables;
using Azure.Storage.Blobs;
using PolyPersist.Net.BlobStore.AzureBlob.AzureTable;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.AzureBlob
{
    internal class AzureBlob_Collection<TEntity> : IBlobCollection<TEntity>
        where TEntity : IEntity, new()
    {
        private BlobContainerClient _containerClient;
        internal TableClient _tableClient;



        public AzureBlob_Collection(BlobContainerClient containerClient, TableClient tableClient)
        {
            _containerClient = containerClient;
            _tableClient = tableClient;
        }

        /// <inheritdoc/>
        string ICollection<TEntity>.Name => _containerClient.Name;

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Insert(TEntity entity)
        {
            await CollectionCommon.CheckBeforeInsert(entity).ConfigureAwait(false);

            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(entity));

            // new etag
            entity.etag = Guid.NewGuid().ToString();

            // set metadata
            await blobClient.SetMetadataAsync(_getMetadata(entity)).ConfigureAwait(false);

            // add all members to table storage
            await _tableClient.AddEntityAsync(new AzureTable_Entity<TEntity>(entity)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(entity));

            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it does not exist.");

            var properties = await blobClient.GetPropertiesAsync().ConfigureAwait(false);
            if (properties.Value.Metadata[nameof(entity.etag)] != entity.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed.");

            // new etag
            entity.etag = Guid.NewGuid().ToString();

            // overwrite metadata
            await blobClient.SetMetadataAsync(_getMetadata(entity)).ConfigureAwait(false);

            // add update table storage
            await _tableClient.UpdateEntityAsync(new AzureTable_Entity<TEntity>(entity), ETag.All).ConfigureAwait(false);
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

            // add delet from table storage
            await _tableClient.DeleteEntityAsync(partitionKey,id, ETag.All).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<TEntity> ICollection<TEntity>.Find(string id, string partitionKey)
        {
            NullableResponse<AzureTable_Entity<TEntity>> tableEnity = await _tableClient.GetEntityIfExistsAsync<AzureTable_Entity<TEntity>>(partitionKey, id).ConfigureAwait(false);

            if (tableEnity.HasValue == true)
                return tableEnity.Value.Entity;

            return default;
        }

        /// <inheritdoc/>
        TQuery ICollection<TEntity>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TEntity>).IsAssignableFrom(typeof(TQuery));
            if (isQueryable == false)
                throw new Exception($"TQuery is must be 'IQueryable<TEntity>' in dotnet implementation");

            AzureTable_Queryable<TableEntity> queryable = new AzureTable_Queryable<TableEntity>(_tableClient);

            return (TQuery)queryable.AsQueryable();
        }

        /// <inheritdoc/>
        object ICollection<TEntity>.GetUnderlyingImplementation()
        {
            return _containerClient;
        }

        /// <inheritdoc/>
        async Task IBlobCollection<TEntity>.UploadContent(TEntity entity, Stream source)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(entity));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be upload content, because it does not exist.");

            await blobClient.UploadAsync(source, overwrite: true).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobCollection<TEntity>.DownloadContentTo(TEntity entity, Stream destination)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(entity));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be download content, because it does not exist.");

            await blobClient.DownloadToAsync(destination).ConfigureAwait(false);
        }


        private string _makePath(IEntity entity)
            => _makePath(entity.PartitionKey, entity.id);

        private string _makePath(string partitionKey, string id)
            => $"{partitionKey}/{id}";

        private Dictionary<string, string> _getMetadata(TEntity entity)
        {
            IBlob blob = (IBlob)entity;

            return new Dictionary<string, string>() {
                { nameof(blob.id), blob.id },
                { nameof(blob.PartitionKey), blob.PartitionKey },
                { nameof(blob.etag), blob.etag },
                { nameof(blob.fileName), blob.fileName },
                { nameof(blob.contentType), blob.contentType },
            };
        }
    }

    
}
