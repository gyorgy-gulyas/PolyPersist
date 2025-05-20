using Google.Apis.Storage.v1;
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

            await insertRequest.UploadAsync().ConfigureAwait( false );
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

            try
            {
                var getRequest = _storageService.Objects.Get(_bucketName, blob.id);
                var stream = await getRequest.ExecuteAsStreamAsync().ConfigureAwait(false);
                await stream.CopyToAsync(content);
                content.Seek( 0, SeekOrigin.Begin );

                return content;
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{blob.id}' does not exist in bucket '{_bucketName}'", ex);
            }
        }

        public async Task<TBlob> Find(string partitionKey, string id)
        {
            try
            {
                var obj = await _storageService.Objects.Get(_bucketName, id)
                    .ExecuteAsync()
                    .ConfigureAwait( false );

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
                    .ConfigureAwait( false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }
        }

        public async Task UpdateContent(TBlob blob, Stream content)
        {
            // 1. Check if the object exists
            try
            {
                var obj = await _storageService.Objects.Get(_bucketName, blob.id)
                    .ExecuteAsync()
                    .ConfigureAwait(false);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException($"Blob '{blob.id}' does not exist in bucket '{_bucketName}'", ex);
            }

            // 2. Upload new content (overwrites existing)
            var insertRequest = _storageService.Objects.Insert(
                new Google.Apis.Storage.v1.Data.Object
                {
                    Bucket = _bucketName,
                    Name = blob.id,
                    ContentType = blob.contentType ?? "application/octet-stream"
                },
                _bucketName,
                content,
                blob.contentType ?? "application/octet-stream");

            await insertRequest.UploadAsync().ConfigureAwait(false);
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
