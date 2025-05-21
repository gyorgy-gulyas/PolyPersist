using Google.Apis.Download;
using Google.Apis.Storage.v1;
using Google.Apis.Upload;
using PolyPersist.Net.Common;
using System.Text.Json;

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
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            if (string.IsNullOrEmpty(blob.id) == true)
                blob.id = Guid.NewGuid().ToString();
            else if (await _IsExistsInternal(blob.id).ConfigureAwait(false) == true)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} cannot be uploaded, beacuse of duplicate key");

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            content.Seek(0, SeekOrigin.Begin);

            string meta_json = JsonSerializer.Serialize(blob);
            var insertRequest = _storageService.Objects.Insert(
                new Google.Apis.Storage.v1.Data.Object
                {
                    Bucket = _bucketName,
                    Name = blob.id,
                    ContentType = blob.contentType ?? "application/octet-stream",
                    Metadata = new Dictionary<string, string>
                    {
                        [nameof(meta_json)] = meta_json
                    }
                },
                _bucketName,
                content,
                blob.contentType ?? "application/octet-stream");

            var result = await insertRequest.UploadAsync().ConfigureAwait(false);
            if (result.Status != UploadStatus.Completed)
                throw new Exception($"Blob '{blob.id}' cannot be uploaded '{_bucketName}'");
        }

        private async Task<bool> _IsExistsInternal(string id)
        {
            try
            {
                await _storageService.Objects.Get(_bucketName, id)
                    .ExecuteAsync()
                    .ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }

            return true;
        }

        public async Task<Stream> Download(TBlob blob)
        {
            var content = new MemoryStream();

            var getRequest = _storageService.Objects.Get(_bucketName, blob.id);
            var result = await getRequest.DownloadAsync(content).ConfigureAwait(false);

            if (result.Status == DownloadStatus.Failed)
            {
                // Lehetséges, hogy 404 (nem létezik), vagy más hiba
                if (result.Exception is Google.GoogleApiException ex && ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    throw new Exception($"Blob '{blob.id}' does not exist in bucket '{_bucketName}'", ex);
                }

                // Általános hibadobás más hiba esetén
                throw new Exception($"Download failed for blob '{blob.id}' in bucket '{_bucketName}'", result.Exception);
            }

            content.Seek(0, SeekOrigin.Begin);

            return content;
        }

        public async Task<TBlob> Find(string partitionKey, string id)
        {
            try
            {
                var obj = await _storageService.Objects.Get(_bucketName, id)
                    .ExecuteAsync()
                    .ConfigureAwait(false);

                string meta_json = obj.Metadata[nameof(meta_json)];
                var blob = JsonSerializer.Deserialize<TBlob>(meta_json);

                return blob;
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return default;
            }
        }

        public async Task Delete(string partitionKey, string id)
        {
            try
            {
                await _storageService.Objects.Delete(_bucketName, id)
                    .ExecuteAsync()
                    .ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }
        }

        public async Task UpdateContent(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            // 1. Check if the object exists
            Google.Apis.Storage.v1.Data.Object existingObject;
            try
            {
                existingObject = await _storageService.Objects.Get(_bucketName, blob.id)
                    .ExecuteAsync()
                    .ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{blob.id}' does not exist in bucket '{_bucketName}'", ex);
            }

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);

            // 2. Upload new content (overwrites existing)
            var insertRequest = _storageService.Objects.Insert(
                new Google.Apis.Storage.v1.Data.Object
                {
                    Bucket = _bucketName,
                    Name = blob.id,
                    ContentType = blob.contentType ?? "application/octet-stream",
                    Metadata = new Dictionary<string, string>
                    {
                        [nameof(meta_json)] = meta_json
                    }
                },
                _bucketName,
                content,
                blob.contentType ?? "application/octet-stream");

            var result = await insertRequest.UploadAsync().ConfigureAwait(false);
            if (result.Status != UploadStatus.Completed)
                throw new Exception($"Blob '{blob.id}' cannot be uploaded '{_bucketName}'");
        }

        public async Task UpdateMetadata(TBlob blob)
        {
            Google.Apis.Storage.v1.Data.Object existingObject;

            try
            {
                existingObject = await _storageService.Objects.Get(_bucketName, blob.id).ExecuteAsync();
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Cannot update metadata because the object '{blob.id}' does not exist in bucket '{_bucketName}'", ex);
            }

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            // Patch metadata
            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            existingObject.Metadata = new Dictionary<string, string>
            {
                [nameof(meta_json)] = meta_json,
            };

            var patchRequest = _storageService.Objects.Patch(existingObject, _bucketName, blob.id);
            await patchRequest.ExecuteAsync().ConfigureAwait(false);
        }

        public object GetUnderlyingImplementation() => _storageService;
    }
}
