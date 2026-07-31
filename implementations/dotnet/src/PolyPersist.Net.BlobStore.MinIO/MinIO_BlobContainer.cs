using Minio;
using Minio.DataModel;
using Minio.DataModel.Args;
using Minio.Exceptions;
using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.MinIO
{
    internal class MinIO_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private readonly string _bucketName;
        private readonly IMinioClient _minioClient;
        private readonly MinIO_BlobStore _store;

        public MinIO_BlobContainer(string containerName, IMinioClient minioClient, MinIO_BlobStore store)
        {
            _bucketName = containerName;
            _minioClient = minioClient;
            _store = store;
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _bucketName;
        /// <inheritdoc/>
        IStore IBlobContainer<TBlob>.ParentStore => _store;

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new InvalidRequestException($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            CollectionCommon.CheckBeforeInsert(blob);

            // Only a caller-supplied id can already be taken; a generated one cannot.
            if (string.IsNullOrEmpty(blob.id) == false && await _IsExistsInternal(blob.id).ConfigureAwait(false) == true)
                throw new DuplicateKeyException($"Blob '{typeof(TBlob).Name}' {blob.id} cannot be uploaded, beacuse of duplicate key");

            CollectionCommon.StampForInsert(blob);

            content.Seek(0, SeekOrigin.Begin);

            string meta_json = BlobMetadata.Serialize(blob);
            var metadata = new Dictionary<string, string> {
                [BlobMetadata.Key] = meta_json,
            };

            await _minioClient.PutObjectAsync(new PutObjectArgs()
                 .WithBucket(_bucketName)
                 .WithObject(blob.id)
                 .WithStreamData(content)
                 .WithObjectSize(content.Length)
                 .WithContentType(blob.contentType ?? "application/octet-stream")
                 .WithHeaders(metadata)).ConfigureAwait(false);
        }

        private async Task<bool> _IsExistsInternal(string id)
        {
            try
            {
                await _minioClient.StatObjectAsync(new StatObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(id)).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException)
            {
                return false;
            }

            return true;
        }

        /// <inheritdoc/>
        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            var content = new MemoryStream();

            try
            {
                await _minioClient.GetObjectAsync(new GetObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(blob.id)
                    .WithCallbackStream(stream => stream.CopyTo(content))).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException ex)
            {
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist", ex);
            }

            content.Position = 0;
            return content;
        }

        /// <inheritdoc/>
        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            ObjectStat stat;
            try
            {
                stat = await _minioClient.StatObjectAsync(new StatObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(id)).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException)
            {
                return default(TBlob)!;
            }

            string meta_json = stat.MetaData[BlobMetadata.Key];
            var blob = BlobMetadata.Deserialize<TBlob>(meta_json);

            // (partitionKey, id) identifies the blob: a matching id in another partition is not it.
            if (blob.PartitionKey != partitionKey)
                return default(TBlob)!;

            return blob;
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            ObjectStat stat;
            try
            {
                stat = await _minioClient.StatObjectAsync(new StatObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(id)).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException)
            {
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {id} cannot be deleted: it does not exist.");
            }

            // Only delete within the requested partition: a matching id in another partition is not it.
            if (BlobMetadata.Deserialize<TBlob>(stat.MetaData[BlobMetadata.Key]).PartitionKey != partitionKey)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {id} cannot be deleted: it does not exist.");

            await _minioClient.RemoveObjectAsync(new RemoveObjectArgs()
                .WithBucket(_bucketName)
                .WithObject(id)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            if (content == null || content.CanRead == false)
                throw new InvalidRequestException($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            ObjectStat stat;
            try
            {
                stat = await _minioClient.StatObjectAsync(new StatObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(blob.id)).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException ex)
            {
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is does not exist", ex);
            }

            var stored = BlobMetadata.Deserialize<TBlob>(stat.MetaData[BlobMetadata.Key]);
            // A matching id in another partition is not this blob - refuse to write across it.
            if (stored.PartitionKey != blob.PartitionKey)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is does not exist");
            CollectionCommon.CheckEtagMatch(stored, blob);

            CollectionCommon.StampForUpdate(blob);
            string meta_json = BlobMetadata.Serialize(blob);
            var metadata = new Dictionary<string, string>
            {
                [BlobMetadata.Key] = meta_json,
            };

            content.Seek(0, SeekOrigin.Begin);
            await _minioClient.PutObjectAsync(new PutObjectArgs()
                 .WithBucket(_bucketName)
                 .WithObject(blob.id)
                 .WithStreamData(content)
                 .WithObjectSize(content.Length)
                 .WithContentType(blob.contentType ?? "application/octet-stream")
                 .WithHeaders(metadata)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            var content = new MemoryStream();

            ObjectStat stat;
            try
            {
                stat = await _minioClient.GetObjectAsync(new GetObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(blob.id)
                    .WithCallbackStream(stream => stream.CopyTo(content))).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException ex)
            {
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is does not exist", ex);
            }

            var stored = BlobMetadata.Deserialize<TBlob>(stat.MetaData[BlobMetadata.Key]);
            // A matching id in another partition is not this blob - refuse to write across it.
            if (stored.PartitionKey != blob.PartitionKey)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is does not exist");
            CollectionCommon.CheckEtagMatch(stored, blob);

            CollectionCommon.StampForUpdate(blob);
            string meta_json = BlobMetadata.Serialize(blob);
            var metadata = new Dictionary<string, string>
            {
                [BlobMetadata.Key] = meta_json,
            };

            content.Seek(0, SeekOrigin.Begin);
            await _minioClient.PutObjectAsync(new PutObjectArgs()
                 .WithBucket(_bucketName)
                 .WithObject(blob.id)
                 .WithStreamData(content)
                 .WithObjectSize(content.Length)
                 .WithContentType(blob.contentType ?? "application/octet-stream")
                 .WithHeaders(metadata)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _minioClient;
        }
    }



}
