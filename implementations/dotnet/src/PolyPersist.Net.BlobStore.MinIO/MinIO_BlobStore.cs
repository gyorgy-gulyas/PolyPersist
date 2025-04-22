using Minio;
using Minio.DataModel.Args;

namespace PolyPersist.Net.BlobStore.MinIO
{
    internal class MinIO_BlobStore : IBlobStore
    {
        internal IMinioClient _minioClient;

        public MinIO_BlobStore(string connectionString)
        {
            var config = MinioConnectionStringParser.Parse(connectionString);

            IMinioClient client = new MinioClient()
                .WithEndpoint(config.Endpoint)
                .WithCredentials(config.AccessKey, config.SecretKey);
            if (config.WithSSL)
                client.WithSSL();

            _minioClient = client.Build();
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "MinIO_Blobs";

        /// <inheritdoc/>
        async Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
           return await _minioClient.BucketExistsAsync(new BucketExistsArgs().WithBucket(containerName)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            if (await _minioClient.BucketExistsAsync(new BucketExistsArgs().WithBucket(containerName)).ConfigureAwait(false) == true)
                throw new Exception($"Container '{containerName}' is already exist in MinIO Blob Store");

            await _minioClient.MakeBucketAsync(new MakeBucketArgs().WithBucket(containerName));

            return new MinIO_BlobContainer<TBlob>(containerName, _minioClient);
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            if (await _minioClient.BucketExistsAsync(new BucketExistsArgs().WithBucket(containerName)).ConfigureAwait(false) == false)
                throw new Exception($"Container '{containerName}' does not exist in Azure Storage");

            return new MinIO_BlobContainer<TBlob>(containerName,_minioClient);
        }

        /// <inheritdoc/>
        async Task IBlobStore.DropContainer(string containerName)
        {
            if (await _minioClient.BucketExistsAsync(new BucketExistsArgs().WithBucket(containerName)).ConfigureAwait(false) == false)
                throw new Exception($"Container '{containerName}' does not exist in Azure Storage");

            // remove all objects in bucket before deleting
            // MinIO does not allow removing non-empty buckets
            var listArgs = new ListObjectsArgs().WithBucket(containerName).WithRecursive(true);
            await foreach (var item in _minioClient.ListObjectsEnumAsync(listArgs))
            {
                await _minioClient.RemoveObjectAsync(new RemoveObjectArgs()
                    .WithBucket(containerName)
                    .WithObject(item.Key));
            }

            await _minioClient.RemoveBucketAsync(new RemoveBucketArgs().WithBucket(containerName));
        }
    }

    public class MinioConnectionInfo
    {
        public string Type { get; set; }
        public string Endpoint { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public bool WithSSL { get; set; }
    }

    public static class MinioConnectionStringParser
    {
        public static MinioConnectionInfo Parse(string connectionString)
        {
            var result = new MinioConnectionInfo();
            var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in parts)
            {
                var kv = part.Split('=', 2);
                if (kv.Length == 2)
                {
                    dict[kv[0].Trim()] = kv[1].Trim();
                }
            }

            dict.TryGetValue("type", out var type);
            dict.TryGetValue("endpoint", out var endpoint);
            dict.TryGetValue("access-key", out var accessKey);
            dict.TryGetValue("secret-key", out var secretKey);
            dict.TryGetValue("withssl", out var withSslStr);

            return new MinioConnectionInfo
            {
                Type = type,
                Endpoint = endpoint,
                AccessKey = accessKey,
                SecretKey = secretKey,
                WithSSL = bool.TryParse(withSslStr, out var withSsl) && withSsl
            };
        }
    }
}
