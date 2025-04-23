using Google.Cloud.Storage.V1;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.GoogleCloudStorage
{
    internal class GoogleCloudStorage_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private readonly string _bucketName;
        internal StorageClient _gcsClient;

        public GoogleCloudStorage_BlobContainer(string containerName, StorageClient gcsClient)
        {
            _bucketName = containerName;
            _gcsClient = gcsClient;
        }

        string IBlobContainer<TBlob>.Name => _bucketName;

        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            var key = BuildKey(blob.PartitionKey, blob.id);

            var obj = await _gcsClient.UploadObjectAsync(
                bucket: _bucketName,
                objectName: key,
                contentType: blob.contentType ?? "application/octet-stream",
                source: content,
                options: new UploadObjectOptions { },
                cancellationToken: default).ConfigureAwait(false);

            var metadata = MetadataHelper.GetMetadata(blob);
            if (metadata != null && metadata.Count > 0)
            {
                obj.Metadata ??= new Dictionary<string, string>();
                foreach (var kv in metadata)
                {
                    obj.Metadata[kv.Key] = kv.Value;
                }
                await _gcsClient.UpdateObjectAsync(obj).ConfigureAwait(false);
            }
        }

        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);
            var ms = new MemoryStream();
            await _gcsClient.DownloadObjectAsync(_bucketName, key, ms).ConfigureAwait(false);
            ms.Position = 0;
            return ms;
        }

        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            var key = BuildKey(partitionKey, id);
            try
            {
                var obj = await _gcsClient.GetObjectAsync(_bucketName, key);
                var blob = new TBlob
                {
                    id = id,
                    PartitionKey = partitionKey,
                    contentType = obj.ContentType
                };

                if (obj.Metadata is Dictionary<string, string> metadata)
                {
                    MetadataHelper.SetMetadata(blob, metadata);
                }
                return blob;
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return default(TBlob);
            }
        }

        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            var key = BuildKey(partitionKey, id);
            try
            {
                await _gcsClient.DeleteObjectAsync(_bucketName, key).ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }
        }

        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            var objectName = BuildKey(blob.PartitionKey, blob.id);

            // 1. Check if the object exists
            try
            {
                var obj = await _gcsClient.GetObjectAsync(_bucketName, objectName).ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException($"Blob '{objectName}' does not exist in bucket '{_bucketName}'", ex);
            }

            // 2. Upload new content (overwrites existing)
            await _gcsClient.UploadObjectAsync(
                bucket: _bucketName,
                objectName: objectName,
                contentType: blob.contentType ?? "application/octet-stream",
                source: content).ConfigureAwait(false);
        }

        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            var objectName = BuildKey(blob.PartitionKey, blob.id);

            Google.Apis.Storage.v1.Data.Object existingObject;

            // 1. Ensure the object exists
            try
            {
                existingObject = await _gcsClient.GetObjectAsync(_bucketName, objectName).ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException($"Cannot update metadata because the object '{objectName}' does not exist in bucket '{_bucketName}'", ex);
            }

            // 2. Download the existing content
            var memoryStream = new MemoryStream();
            await _gcsClient.DownloadObjectAsync(_bucketName, objectName, memoryStream).ConfigureAwait(false);
            memoryStream.Position = 0;

            // 3. Re-upload with new metadata and same content
            var newObject = new Google.Apis.Storage.v1.Data.Object
            {
                Bucket = _bucketName,
                Name = objectName,
                ContentType = blob.contentType ?? existingObject.ContentType,
                Metadata = MetadataHelper.GetMetadata(blob)
            };

            await _gcsClient.UploadObjectAsync(newObject, memoryStream).ConfigureAwait(false);
        }

        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _gcsClient;
        }

        private string BuildKey(string partitionKey, string id)
        {
            return $"{partitionKey}/{id}";
        }
    }
}
