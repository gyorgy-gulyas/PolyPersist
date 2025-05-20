using Amazon.S3;
using Amazon.S3.Model;
using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.AmazonS3
{
    internal class AmazonS3_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private readonly string _bucketName;
        internal IAmazonS3 _amazonS3Client;


        public AmazonS3_BlobContainer(string containerName, IAmazonS3 amazonS3Client)
        {
            _bucketName = containerName;
            _amazonS3Client = amazonS3Client;
        }

        string IBlobContainer<TBlob>.Name => _bucketName;

        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
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

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = blob.id,
                InputStream = content,
                ContentType = blob.contentType ?? "application/octet-stream"
            };

            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            request.Metadata[nameof(meta_json)] = meta_json;

            await _amazonS3Client.PutObjectAsync(request).ConfigureAwait(false);
        }

        private async Task<bool> _IsExistsInternal(string id)
        {
            try
            {
                await _amazonS3Client
                    .GetObjectAsync(_bucketName, id)
                    .ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }

            return true;
        }

        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            try
            {
                var response = await _amazonS3Client.GetObjectAsync(_bucketName, blob.id).ConfigureAwait(false);

                var ms = new MemoryStream((int)response.ContentLength);
                await response.ResponseStream.CopyToAsync(ms);
                ms.Position = 0;
                return ms;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist");
            }
        }

        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            try
            {
                var @object = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, id).ConfigureAwait(false);
                string meta_json = @object.Metadata[nameof(meta_json)];
                var blob = JsonSerializer.Deserialize<TBlob>(meta_json);

                return blob;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return default(TBlob);
            }
        }

        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            try
            {
                await _amazonS3Client.GetObjectMetadataAsync(_bucketName, id).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }

            await _amazonS3Client.DeleteObjectAsync(_bucketName, id).ConfigureAwait(false);
        }

        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            GetObjectMetadataResponse response;
            try
            {
                response = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, blob.id).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");
            }

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = blob.id,
                InputStream = content,
                ContentType = blob.contentType
            };

            // copy original metadata
            foreach (var kv in MetadataConverter.ToDictionary(response.Metadata))
            {
                request.Metadata[kv.Key] = kv.Value;
            }

            await _amazonS3Client.PutObjectAsync(request).ConfigureAwait(false);
        }

        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            // 1. Ensure the object exists in S3
            GetObjectMetadataResponse metadata;
            try
            {
                metadata = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, blob.id).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");
            }

            // 2. Download the existing object content
            var originalStream = new MemoryStream();
            await _amazonS3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucketName,
                Key = blob.id
            }).ContinueWith(async task =>
            {
                using var response = await task;
                await response.ResponseStream.CopyToAsync(originalStream);
            }).Unwrap();

            originalStream.Position = 0;

            // 3. Re-upload the object with the same content and updated metadata
            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = blob.id,
                InputStream = originalStream,
                ContentType = blob.contentType ?? metadata.Headers.ContentType ?? "application/octet-stream"
            };

            // 4. Apply the new user-defined metadata from the blob
            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            request.Metadata[nameof(meta_json)] = meta_json;


            // 5. Upload the updated object back to S3
            await _amazonS3Client.PutObjectAsync(request).ConfigureAwait(false);
        }

        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _amazonS3Client;
        }
    }

    public static class MetadataConverter
    {
        public static Dictionary<string, string> ToDictionary(MetadataCollection metadata)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var key in metadata.Keys)
            {
                dict[key] = metadata[key];
            }

            return dict;
        }
    }
}