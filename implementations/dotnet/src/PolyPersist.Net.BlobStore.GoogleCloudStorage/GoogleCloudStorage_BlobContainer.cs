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
                cancellationToken: default);

            var metadata = MetadataHelper.GetMetadata(blob);
            if (metadata != null && metadata.Count > 0)
            {
                obj.Metadata ??= new Dictionary<string, string>();
                foreach (var kv in metadata)
                {
                    obj.Metadata[kv.Key] = kv.Value;
                }
                await _gcsClient.UpdateObjectAsync(obj);
            }
        }

        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);
            var ms = new MemoryStream();
            await _gcsClient.DownloadObjectAsync(_bucketName, key, ms);
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
                await _gcsClient.DeleteObjectAsync(_bucketName, key);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }
        }

        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            await ((IBlobContainer<TBlob>)this).Upload(blob, content);
        }

        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);
            var obj = await _gcsClient.GetObjectAsync(_bucketName, key);
            var ms = new MemoryStream();
            await _gcsClient.DownloadObjectAsync(_bucketName, key, ms);
            ms.Position = 0;
            await ((IBlobContainer<TBlob>)this).Upload(blob, ms);
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
