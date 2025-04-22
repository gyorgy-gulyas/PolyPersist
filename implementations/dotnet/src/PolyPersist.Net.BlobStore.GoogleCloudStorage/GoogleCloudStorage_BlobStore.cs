using Google.Cloud.Storage.V1;

namespace PolyPersist.Net.BlobStore.GoogleCloudStorage
{
    internal class GoogleCloudStorageBlobStore : IBlobStore
    {
        private readonly string _storeName;
        internal readonly StorageClient _gcsClient;

        public GoogleCloudStorageBlobStore(string storeName, string connectionString)
        {
            _storeName = storeName;
            var config = GoogleCloudStorageConnectionStringParser.Parse(connectionString);

            var builder = new StorageClientBuilder();
            if (!string.IsNullOrWhiteSpace(config.JsonCredentialPath))
            {
                builder.CredentialsPath = config.JsonCredentialPath;
            }
            _gcsClient = builder.Build();
        }

        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        string IStore.ProviderName => "GCS_Blobs";
        string IStore.Name => _storeName;

        async Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            try
            {
                var bucket = await _gcsClient.GetBucketAsync(containerName);
                return bucket != null;
            }
            catch
            {
                return false;
            }
        }

        async Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            if (await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' already exists in blob storage '{_storeName}'");

            await _gcsClient.CreateBucketAsync("your-project-id", new Google.Apis.Storage.v1.Data.Bucket
            {
                Name = containerName
            });

            return new GoogleCloudStorage_BlobContainer<TBlob>(containerName, _gcsClient);
        }

        async Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            if (!await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' does not exist in blob storage '{_storeName}'");

            return new GoogleCloudStorage_BlobContainer<TBlob>(containerName, _gcsClient);
        }

        async Task IBlobStore.DropContainer(string containerName)
        {
            if (!await ((IBlobStore)this).IsContainerExists(containerName).ConfigureAwait(false))
                throw new Exception($"Container '{containerName}' does not exist in blob storage '{_storeName}'");

            var objects = _gcsClient.ListObjects(containerName, "");
            foreach (var obj in objects)
            {
                await _gcsClient.DeleteObjectAsync(containerName, obj.Name);
            }

            await _gcsClient.DeleteBucketAsync(containerName);
        }
    }

    public class GoogleCloudStorageConnectionInfo
    {
        public string Type { get; set; }
        public string JsonCredentialPath { get; set; }
    }

    public static class GoogleCloudStorageConnectionStringParser
    {
        public static GoogleCloudStorageConnectionInfo Parse(string connectionString)
        {
            var result = new GoogleCloudStorageConnectionInfo();
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
            dict.TryGetValue("jsoncredentialpath", out var jsonCredentialPath);

            return new GoogleCloudStorageConnectionInfo
            {
                Type = type,
                JsonCredentialPath = jsonCredentialPath
            };
        }
    }
}
