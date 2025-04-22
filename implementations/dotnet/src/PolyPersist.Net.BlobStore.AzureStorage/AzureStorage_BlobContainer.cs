using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.AzureStorage
{
    internal class AzureStorage_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private BlobContainerClient _containerClient;

        public AzureStorage_BlobContainer(BlobContainerClient containerClient)
        {
            _containerClient = containerClient;
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _containerClient.Name;

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));

            // new etag
            blob.etag = Guid.NewGuid().ToString();

            // set metadata
            await blobClient.SetMetadataAsync(MetadataHelper.GetMetadata(blob)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be download because it does not exist.");

            // Download blob content
            var response = await blobClient.DownloadAsync();
            return response.Value.Content;
        }

        /// <inheritdoc/>
        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(partitionKey,id));

            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
               return default(TBlob);

            var properties = await blobClient.GetPropertiesAsync().ConfigureAwait(false);
            // Create a new instance of the target type
            var blob = new TBlob();

            return MetadataHelper.SetMetadata<TBlob>(blob,properties.Value.Metadata);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(partitionKey, id));

            var response = await blobClient.DeleteIfExistsAsync().ConfigureAwait(false);
            if(response.Value == false )
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be delete because it does not exist.");
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            // Upload new content (overwrite)
            await blobClient.UploadAsync(content, overwrite: true);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            // set metadata
            await blobClient.SetMetadataAsync(MetadataHelper.GetMetadata(blob)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _containerClient;
        }

        private string _makePath(IEntity entity)
            => _makePath(entity.PartitionKey, entity.id);

        private string _makePath(string partitionKey, string id)
            => $"{partitionKey}/{id}";
    }
}
