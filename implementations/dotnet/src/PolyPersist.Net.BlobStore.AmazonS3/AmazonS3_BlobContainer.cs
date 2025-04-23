using Amazon.S3;
using Amazon.S3.Model;
using PolyPersist.Net.Common;

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
            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            var key = BuildKey(blob.PartitionKey, blob.id);

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = key,
                InputStream = content,
                ContentType = blob.contentType ?? "application/octet-stream"
            };

            foreach (var kv in MetadataHelper.GetMetadata(blob))
                request.Metadata[kv.Key] = kv.Value;

            await _amazonS3Client.PutObjectAsync(request).ConfigureAwait(false);
        }

        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);

            try
            {
                var response = await _amazonS3Client.GetObjectAsync(_bucketName, key).ConfigureAwait(false);

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
            var key = BuildKey(partitionKey, id);
            try
            {
                var @object = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, key).ConfigureAwait(false);
                var blob = new TBlob
                {
                    id = id,
                    PartitionKey = partitionKey,
                    contentType = @object.Headers.ContentType
                };
                MetadataHelper.SetMetadata(blob, MetadataConverter.ToDictionary(@object.Metadata) );
                return blob;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return default(TBlob);
            }
        }

        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            var key = BuildKey(partitionKey, id);
            try
            {
                await _amazonS3Client.GetObjectMetadataAsync(_bucketName, key).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be deleted because it does not exist.");
            }

            await _amazonS3Client.DeleteObjectAsync(_bucketName, key).ConfigureAwait(false);
        }

        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);

            GetObjectMetadataResponse response;
            try
            {
                response = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, key).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");
            }

            var request = new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = key,
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
            var key = BuildKey(blob.PartitionKey, blob.id);

            // 1. Ensure the object exists in S3
            GetObjectMetadataResponse metadata;
            try
            {
                metadata = await _amazonS3Client.GetObjectMetadataAsync(_bucketName, key).ConfigureAwait(false);
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
                Key = key
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
                Key = key,
                InputStream = originalStream,
                ContentType = blob.contentType ?? metadata.Headers.ContentType ?? "application/octet-stream"
            };

            // 4. Apply the new user-defined metadata from the blob
            foreach (var kv in MetadataHelper.GetMetadata(blob))
            {
                request.Metadata[kv.Key] = kv.Value;
            }

            // 5. Upload the updated object back to S3
            await _amazonS3Client.PutObjectAsync(request).ConfigureAwait(false);
        }

        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _amazonS3Client;
        }

        private string BuildKey(string partitionKey, string id)
        {
            return $"{partitionKey}/{id}";
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