using Azure.Storage.Blobs;

namespace PolyPersist.Net.BlobStore.AzureStorage
{
    internal class AzureStorage_BlobStore : IBlobStore
    {
        private string _storeName;
        internal BlobServiceClient _blobServiceClient;

        public AzureStorage_BlobStore(string storeName, string connectionString)
        {
            _storeName = storeName;
            _blobServiceClient = new BlobServiceClient(connectionString);
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "AzureStorage_Blobs";
        /// <inheritdoc/>
        string IStore.Name => _storeName;

        /// <inheritdoc/>
        async Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, containerName));
            return await containerClient.ExistsAsync();
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, containerName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == true)
                throw new Exception($"Container '{containerName}' is already exist in blob storage '{_storeName}'");

            await containerClient.CreateAsync().ConfigureAwait(false);
            return new AzureStorage_BlobContainer<TBlob>(containerClient);
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, containerName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Container '{containerName}' does not exist in blob storage '{_storeName}'");

            return new AzureStorage_BlobContainer<TBlob>(containerClient);
        }

        /// <inheritdoc/>
        async Task IBlobStore.DropContainer(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, containerName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Container '{containerName}' does not exist in blob storage '{_storeName}'");

            await containerClient.DeleteAsync().ConfigureAwait(false);
        }

        private string _containerName(string storeName, string containerName)
            => $"{storeName}.{containerName}";
    }
}
