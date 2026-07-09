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
        private readonly IAmazonS3 _amazonS3Client;
        private readonly AmazonS3_BlobStore _store;


        public AmazonS3_BlobContainer(string containerName, IAmazonS3 amazonS3Client, AmazonS3_BlobStore store)
        {
            _bucketName = containerName;
            _amazonS3Client = amazonS3Client;
            _store = store;
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _bucketName;
        /// <inheritdoc/>
        IStore IBlobContainer<TBlob>.ParentStore => _store;

        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new PolyPersist.Net.Common.InvalidRequestException($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            CollectionCommon.CheckBeforeInsert(blob);

            if (string.IsNullOrEmpty(blob.id) == true)
                blob.id = Guid.NewGuid().ToString();
            else if (await _IsExistsInternal(blob.id).ConfigureAwait(false) == true)
                throw new DuplicateKeyException($"Blob '{typeof(TBlob).Name}' {blob.id} cannot be uploaded, beacuse of duplicate key");

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = blob.id,
                InputStream = content,
                ContentType = blob.contentType ?? "application/octet-stream"
            };

            string meta_json = BlobMetadata.Serialize(blob);
            request.Metadata[BlobMetadata.Key] = meta_json;

            await _amazonS3Client.PutObjectAsync(request).ConfigureAwait(false);
        }

        private async Task<bool> _IsExistsInternal(string id)
        {
            try
            {
                // HEAD (metadata only) - do not download the object body just to test existence.
                await _amazonS3Client
                    .GetObjectMetadataAsync(_bucketName, id)
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
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist");
            }
        }

        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            try
            {
                var @object = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, id).ConfigureAwait(false);
                string meta_json = @object.Metadata[BlobMetadata.Key];
                var blob = BlobMetadata.Deserialize<TBlob>(meta_json);

                // (partitionKey, id) identifies the blob: a matching id in another partition is not it.
                if (blob.PartitionKey != partitionKey)
                    return default(TBlob)!;

                return blob;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return default(TBlob)!;
            }
        }

        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            GetObjectMetadataResponse meta;
            try
            {
                meta = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, id).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }

            // Only delete within the requested partition: a matching id in another partition is not it.
            if (BlobMetadata.Deserialize<TBlob>(meta.Metadata[BlobMetadata.Key]).PartitionKey != partitionKey)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");

            await _amazonS3Client.DeleteObjectAsync(_bucketName, id).ConfigureAwait(false);
        }

        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            if (content == null || content.CanRead == false)
                throw new PolyPersist.Net.Common.InvalidRequestException($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            GetObjectMetadataResponse response;
            try
            {
                response = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, blob.id).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");
            }

            var stored = BlobMetadata.Deserialize<TBlob>(response.Metadata[BlobMetadata.Key]);
            // A matching id in another partition is not this blob - refuse to write across it.
            if (stored.PartitionKey != blob.PartitionKey)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");
            CollectionCommon.CheckEtagMatch(stored, blob);

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = blob.id,
                InputStream = content,
                ContentType = blob.contentType
            };

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            string meta_json = BlobMetadata.Serialize(blob);
            request.Metadata[BlobMetadata.Key] = meta_json;

            await _amazonS3Client.PutObjectAsync(request).ConfigureAwait(false);
        }

        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            // 1. Ensure the object exists in S3
            GetObjectMetadataResponse metadata;
            try
            {
                metadata = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, blob.id).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");
            }

            var stored = BlobMetadata.Deserialize<TBlob>(metadata.Metadata[BlobMetadata.Key]);
            // A matching id in another partition is not this blob - refuse to write across it.
            if (stored.PartitionKey != blob.PartitionKey)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");
            CollectionCommon.CheckEtagMatch(stored, blob);

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            string meta_json = BlobMetadata.Serialize(blob);

            // Server-side self-copy with REPLACE: updates the object's metadata without downloading
            // and re-uploading its body.
            var copy = new CopyObjectRequest
            {
                SourceBucket = _bucketName,
                SourceKey = blob.id,
                DestinationBucket = _bucketName,
                DestinationKey = blob.id,
                MetadataDirective = S3MetadataDirective.REPLACE,
                ContentType = blob.contentType ?? metadata.Headers.ContentType ?? "application/octet-stream",
            };
            copy.Metadata[BlobMetadata.Key] = meta_json;

            await _amazonS3Client.CopyObjectAsync(copy).ConfigureAwait(false);
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
