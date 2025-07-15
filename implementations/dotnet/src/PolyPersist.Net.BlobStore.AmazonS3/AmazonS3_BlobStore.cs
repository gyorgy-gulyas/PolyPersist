using Amazon.S3;
using Amazon.S3.Model;

namespace PolyPersist.Net.BlobStore.AmazonS3
{
    public class AmazonS3_BlobStore : IBlobStore
    {
        internal readonly IAmazonS3 _s3Client;

        public AmazonS3_BlobStore(string connectionString)
        {
            var config = AmazonS3ConnectionStringParser.Parse(connectionString);

            _s3Client = new AmazonS3Client(config.AccessKey, config.SecretKey, new AmazonS3Config
            {
                ServiceURL = config.Endpoint,
                ForcePathStyle = true,
                UseHttp = !config.WithSSL
            });
        }

        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        string IStore.ProviderName => "S3_Blobs";

        async Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            try
            {
                var response = await _s3Client.ListBucketsAsync();
                return response.Buckets.Any(b => b.BucketName == containerName);
            }
            catch
            {
                return false;
            }
        }

        async Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            if (await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' already exists in AmazonS3");

            await _s3Client.PutBucketAsync(new PutBucketRequest
            {
                BucketName = containerName,
                UseClientRegion = true
            });

            return new AmazonS3_BlobContainer<TBlob>(containerName, _s3Client, this);
        }

        async Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            if (!await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' does not exist in AmazonS3");

            return new AmazonS3_BlobContainer<TBlob>(containerName, _s3Client, this);
        }

        async Task IBlobStore.DropContainer(string containerName)
        {
            if (!await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' does not exist in AmazonS3");

            var listResponse = await _s3Client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = containerName
            });

            foreach (var s3Object in listResponse.S3Objects)
            {
                await _s3Client.DeleteObjectAsync(containerName, s3Object.Key);
            }

            await _s3Client.DeleteBucketAsync(containerName);
        }
    }

    public class AmazonS3ConnectionInfo
    {
        public string Type { get; set; }
        public string Endpoint { get; set; }
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public bool WithSSL { get; set; }
    }

    public static class AmazonS3ConnectionStringParser
    {
        public static AmazonS3ConnectionInfo Parse(string connectionString)
        {
            var result = new AmazonS3ConnectionInfo();
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

            return new AmazonS3ConnectionInfo
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
