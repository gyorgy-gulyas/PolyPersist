using Azure.Storage.Blobs;
using System.Text;

namespace PolyPersist.Net.BlobStore.AzureBlob
{
    internal class AzureBlob_Connection : IConnection
    {
        internal string _connectionString;
        internal BlobServiceClient _blobServiceClient;
        internal BlobContainerClient _systemContainer;

        public AzureBlob_Connection(string connectionString)
        {
            _connectionString = connectionString;
            _blobServiceClient = new BlobServiceClient(connectionString);
            _systemContainer = _blobServiceClient.GetBlobContainerClient("__system");
        }

        /// <inheritdoc/>
        async Task<bool> IConnection.IsDataStoreExists(string storeName)
        {
            BlobClient blobClient = _systemContainer.GetBlobClient(storeName);
            return await blobClient.ExistsAsync();
        }

        /// <inheritdoc/>
        async Task<IDataStore> IConnection.GetDataStoreByName(string storeName)
        {
            BlobClient blobClient = _systemContainer.GetBlobClient(storeName);
            if (await blobClient.ExistsAsync() == false)
                throw new Exception($"DataStore: {storeName} does not exists");

            return new AzureBlob_DataStore(storeName, this);
        }

        /// <inheritdoc/>
        async Task<IDataStore> IConnection.CreateDataStore(string storeName)
        {
            BlobClient blobClient = _systemContainer.GetBlobClient(storeName);
            if (await blobClient.ExistsAsync() == false)
                throw new Exception($"DataStore: {storeName} does not exists");

            string content = $"storeName:{storeName}\ncreationDate:{DateTime.UtcNow}\n";

            using MemoryStream memoryStream = new(Encoding.UTF8.GetBytes(content));
            await blobClient.UploadAsync(memoryStream);

            return new AzureBlob_DataStore(storeName, this);
        }

        /// <inheritdoc/>
        async Task IConnection.DropDataStore(IDataStore dataStore)
        {
            BlobClient blobClient = _systemContainer.GetBlobClient(dataStore.Name);
            if (await blobClient.ExistsAsync() == false)
                throw new Exception($"DataStore: {dataStore.Name} does not exists");

            await _systemContainer.DeleteBlobAsync(dataStore.Name);

            await foreach (var containerItem in _blobServiceClient.GetBlobContainersAsync())
            {
                if (containerItem.Name.StartsWith(dataStore.Name + "."))
                {
                    BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(containerItem.Name);
                    await containerClient.DeleteIfExistsAsync();
                }
            }
        }
    }
}
