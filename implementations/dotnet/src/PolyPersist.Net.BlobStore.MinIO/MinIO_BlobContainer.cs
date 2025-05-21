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

            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            var metadata = new Dictionary<string, string> {
                [nameof(meta_json)] = meta_json,
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
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist", ex);
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
                return default(TBlob);
            }

            string meta_json = stat.MetaData[nameof(meta_json)];
            var blob = JsonSerializer.Deserialize<TBlob>(meta_json);

            return blob;
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            try
            {
                await _minioClient.StatObjectAsync(new StatObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(id)).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} cannot be deleted: it does not exist.");
            }

            await _minioClient.RemoveObjectAsync(new RemoveObjectArgs()
                .WithBucket(_bucketName)
                .WithObject(id)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            try
            {
                await _minioClient.StatObjectAsync(new StatObjectArgs()
                    .WithBucket(_bucketName)
                    .WithObject(blob.id)).ConfigureAwait(false);
            }
            catch (ObjectNotFoundException ex)
            {
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is does dot exist", ex);
            }

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            var metadata = new Dictionary<string, string>
            {
                [nameof(meta_json)] = meta_json,
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
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is does dot exist", ex);
            }

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            string meta_json = System.Text.Json.JsonSerializer.Serialize(blob);
            var metadata = new Dictionary<string, string>
            {
                [nameof(meta_json)] = meta_json,
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
