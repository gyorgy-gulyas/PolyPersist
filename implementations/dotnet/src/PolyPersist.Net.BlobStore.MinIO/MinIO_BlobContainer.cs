using Minio;
using Minio.DataModel;
using Minio.DataModel.Args;
using Minio.Exceptions;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.MinIO
{
    internal class MinIO_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private readonly string _bucketName;
        internal IMinioClient _minioClient;

        public MinIO_BlobContainer(string containerName, IMinioClient minioClient)
        {
            _bucketName = containerName;
            _minioClient = minioClient;
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _bucketName;

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            var key = BuildKey(blob.PartitionKey, blob.id);

            await _UpdateInternal(blob, key, content).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);
            var ms = new MemoryStream();

            await _minioClient.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_bucketName)
                .WithObject(key)
                .WithCallbackStream(stream => stream.CopyTo(ms))).ConfigureAwait(false);

            ms.Position = 0;
            return ms;
        }

        /// <inheritdoc/>
        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            var key = BuildKey(partitionKey, id);
            StatObjectArgs statArgs = new StatObjectArgs()
                .WithBucket(_bucketName)
                .WithObject(key);

            ObjectStat stat;
            try
            {
                stat = await _minioClient.StatObjectAsync(statArgs).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException)
            {
                return default(TBlob);
            }

            var blob = new TBlob
            {
                id = id,
                PartitionKey = partitionKey,
                contentType = stat.ContentType
            };

            MetadataHelper.SetMetadata(blob, stat.MetaData);

            return blob;
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            var key = BuildKey(partitionKey, id);
            try
            {
                await _minioClient.StatObjectAsync(new StatObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(key)).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} cannot be deleted: it does not exist.");
            }

            await _minioClient.RemoveObjectAsync(new RemoveObjectArgs()
                .WithBucket(_bucketName)
                .WithObject(key)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);

            await _UpdateInternal( blob, key, content ).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            var key = BuildKey(blob.PartitionKey, blob.id);

            // Read existing content
            var content = new MemoryStream();
            await _minioClient.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_bucketName)
                .WithObject(key)
                .WithCallbackStream(stream => stream.CopyTo(content))).ConfigureAwait(false);

            content.Position = 0;
            await _UpdateInternal(blob, key, content);
        }

        private async Task _UpdateInternal(TBlob blob, string key, Stream content)
        {
            await _minioClient.PutObjectAsync(new PutObjectArgs()
                 .WithBucket(_bucketName)
                 .WithObject(key)
                 .WithStreamData(content)
                 .WithObjectSize(content.Length)
                 .WithContentType(blob.contentType ?? "application/octet-stream")
                 .WithHeaders(MetadataHelper.GetMetadata(blob))).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _minioClient;
        }

        private string BuildKey(string partitionKey, string id)
        {
            return $"{partitionKey}/{id}";
        }
    }


    
}
