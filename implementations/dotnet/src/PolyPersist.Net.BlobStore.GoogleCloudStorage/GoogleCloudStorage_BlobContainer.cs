using Google.Apis.Storage.v1;
using Google.Apis.Storage.v1.Data;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.GoogleCloudStorage
{
    internal class GoogleCloudStorage_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private readonly string _bucketName;
        private readonly StorageService _storageService;
        private readonly string _projectId;

        public GoogleCloudStorage_BlobContainer(string bucketName, StorageService storageService, string projectId)
        {
            _bucketName = bucketName;
            _storageService = storageService;
            _projectId = projectId;
        }

        public string Name => _bucketName;

        public async Task Upload(TBlob blob, Stream content)
        {
            await PolyPersist.Net.Common.CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            var key = BuildKey(blob.PartitionKey, blob.id);

            var insertRequest = _storageService.Objects.Insert(
                new Google.Apis.Storage.v1.Data.Object
                {
                    Bucket = _bucketName,
                    Name = key,
                    ContentType = blob.contentType ?? "application/octet-stream",
                    Metadata = MetadataHelper.GetMetadata(blob)
                },
                _bucketName,
                content,
                blob.contentType ?? "application/octet-stream");

            await insertRequest.UploadAsync();
        }

        public async Task<Stream> Download(TBlob blob)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);
            var ms = new MemoryStream();

            try
            {
                var getRequest = _storageService.Objects.Get(_bucketName, key);
                var stream = await getRequest.ExecuteAsStreamAsync();
                await stream.CopyToAsync(ms);
                ms.Position = 0;
                return ms;
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException($"Blob '{key}' does not exist in bucket '{_bucketName}'", ex);
            }
        }

        public async Task<TBlob> Find(string partitionKey, string id)
        {
            var key = BuildKey(partitionKey, id);
            try
            {
                var obj = await _storageService.Objects.Get(_bucketName, key).ExecuteAsync();

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
                return default;
            }
        }

        public async Task Delete(string partitionKey, string id)
        {
            var key = BuildKey(partitionKey, id);
            try
            {
                await _storageService.Objects.Delete(_bucketName, key).ExecuteAsync();
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }
        }

        public async Task UpdateContent(TBlob blob, Stream content)
        {
            var objectName = BuildKey(blob.PartitionKey, blob.id);

            // 1. Check if the object exists
            try
            {
                var obj = await _storageService.Objects.Get(_bucketName, objectName).ExecuteAsync();
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException($"Blob '{objectName}' does not exist in bucket '{_bucketName}'", ex);
            }

            // 2. Upload new content (overwrites existing)
            var insertRequest = _storageService.Objects.Insert(
                new Google.Apis.Storage.v1.Data.Object
                {
                    Bucket = _bucketName,
                    Name = objectName,
                    ContentType = blob.contentType ?? "application/octet-stream"
                },
                _bucketName,
                content,
                blob.contentType ?? "application/octet-stream");

            await insertRequest.UploadAsync();
        }

        public async Task UpdateMetadata(TBlob blob)
        {
            var objectName = BuildKey(blob.PartitionKey, blob.id);

            Google.Apis.Storage.v1.Data.Object existingObject;

            try
            {
                existingObject = await _storageService.Objects.Get(_bucketName, objectName).ExecuteAsync();
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException($"Cannot update metadata because the object '{objectName}' does not exist in bucket '{_bucketName}'", ex);
            }

            // Patch metadata
            existingObject.Metadata = MetadataHelper.GetMetadata(blob);

            var patchRequest = _storageService.Objects.Patch(existingObject, _bucketName, objectName);
            await patchRequest.ExecuteAsync();
        }

        private string BuildKey(string partitionKey, string id)
        {
            return $"{partitionKey}/{id}";
        }

        public object GetUnderlyingImplementation() => _storageService;
    }
}
