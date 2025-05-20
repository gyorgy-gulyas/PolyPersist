using Azure.Storage.Blobs;
using PolyPersist.Net.Common;
using System.Text.Json;

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
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            if (string.IsNullOrEmpty(blob.id) == true)
                blob.id = Guid.NewGuid().ToString();
            else if ( await _FindInternal( blob.id).ConfigureAwait(false) == true)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} cannot be uploaded, beacuse of duplicate key");

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(blob.id);
            content.Seek(0, SeekOrigin.Begin);
            await blobClient.UploadAsync(content).ConfigureAwait(false);

            // set metadata
            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            var metadata = new Dictionary<string, string>
            {
                [nameof(meta_json)] = meta_json,
            };
            await blobClient.SetMetadataAsync(metadata).ConfigureAwait(false);
        }

        private async Task<bool> _FindInternal(string id)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(id);

            // ✅ Ellenőrzés: ha már létezik, dobj kivételt
            return await blobClient.ExistsAsync().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(blob.id);
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
            BlobClient blobClient = _containerClient.GetBlobClient(id);

            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
               return default(TBlob);

            var properties = await blobClient.GetPropertiesAsync().ConfigureAwait(false);
            // Create a new instance of the target type
            string meta_json = properties.Value.Metadata[nameof(meta_json)];
            var blob = JsonSerializer.Deserialize<TBlob>(meta_json);

            return blob;
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(id);

            var response = await blobClient.DeleteIfExistsAsync().ConfigureAwait(false);
            if(response.Value == false )
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} cannot be deleted: it does not exist.");
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(blob.id);
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            // Upload new content (overwrite)
            await blobClient.UploadAsync(content, overwrite: true);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(blob.id);
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            // set metadata
            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            var metadata = new Dictionary<string, string>
            {
                [nameof(meta_json)] = meta_json,
            };
            await blobClient.SetMetadataAsync(metadata).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _containerClient;
        }
    }
}
