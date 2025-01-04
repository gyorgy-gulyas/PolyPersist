﻿using Azure.Data.Tables;
using Azure.Storage.Blobs;

namespace PolyPersist.Net.BlobStore.AzureBlob
{
    internal class AzureBlob_DataStore : IDataStore
    {
        private string _storeName;
        private AzureBlob_Connection _connection;
        internal BlobServiceClient _blobServiceClient;
        internal TableServiceClient _tableServiceClient;

        public AzureBlob_DataStore(string storeName, AzureBlob_Connection connection)
        {
            _storeName = storeName;
            _connection = connection;
            _blobServiceClient = connection._blobServiceClient;
            _tableServiceClient = connection._tableServiceClient;
        }

        /// <inheritdoc/>
        IConnection IDataStore.Connection => _connection;
        /// <inheritdoc/>
        IDataStore.StorageModels IDataStore.StorageModel => IDataStore.StorageModels.BlobStore;
        /// <inheritdoc/>
        string IDataStore.ProviderName => "AzureBlob";
        /// <inheritdoc/>
        string IDataStore.Name => _storeName;

        /// <inheritdoc/>
        async Task<bool> IDataStore.IsCollectionExists(string collectionName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, collectionName));
            return await containerClient.ExistsAsync();
        }

        /// <inheritdoc/>
        async Task<ICollection<TEntity>> IDataStore.CreateCollection<TEntity>(string collectionName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, collectionName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == true)
                throw new Exception($"Collection '{collectionName}' is already exist in AzureBlob storage '{_storeName}'");

            await containerClient.CreateAsync().ConfigureAwait(false);

            var tableClient = _tableServiceClient.GetTableClient(collectionName);
            await tableClient.CreateIfNotExistsAsync().ConfigureAwait(false);

            return new AzureBlob_Collection<TEntity>(containerClient, tableClient);
        }

        /// <inheritdoc/>
        async Task<ICollection<TEntity>> IDataStore.GetCollectionByName<TEntity>(string collectionName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, collectionName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Collection '{collectionName}' does not exist in AzureBlob storage '{_storeName}'");

            var tableClient = _tableServiceClient.GetTableClient(collectionName);
            await tableClient.CreateIfNotExistsAsync().ConfigureAwait(false);

            return new AzureBlob_Collection<TEntity>(containerClient, tableClient);
        }

        /// <inheritdoc/>
        async Task IDataStore.DropCollection(string collectionName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, collectionName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Collection '{collectionName}' does not exist in AzureBlob storage '{_storeName}'");

            await containerClient.DeleteAsync().ConfigureAwait(false);

            var tableClient = _tableServiceClient.GetTableClient(collectionName);
            await tableClient.DeleteAsync().ConfigureAwait(false);

        }

        private string _containerName(string storeName, string collectionName)
            => $"{storeName}.{collectionName}";
    }
}
