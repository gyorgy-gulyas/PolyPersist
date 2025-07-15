using Azure.Storage.Blobs;

namespace PolyPersist.Net.BlobStore.AzureStorage
{
    public class AzureStorage_BlobStore : IBlobStore
    {
        internal BlobServiceClient _blobServiceClient;

        public AzureStorage_BlobStore(string connectionString)
        {
            _blobServiceClient = new BlobServiceClient(connectionString);
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "AzureStorage_Blobs";

        /// <inheritdoc/>
        async Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
            return await containerClient.ExistsAsync();
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == true)
                throw new Exception($"Container '{containerName}' is already exist in Azure Storage");

            await containerClient.CreateAsync().ConfigureAwait(false);
            return new AzureStorage_BlobContainer<TBlob>(containerClient, this);
        }

        /// <inheritdoc/>
        async Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Container '{containerName}' does not exist in Azure Storage");

            return new AzureStorage_BlobContainer<TBlob>(containerClient, this);
        }

        /// <inheritdoc/>
        async Task IBlobStore.DropContainer(string containerName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerName);
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Container '{containerName}' does not exist in Azure Storage");

            await containerClient.DeleteAsync().ConfigureAwait(false);
        }
    }
}
