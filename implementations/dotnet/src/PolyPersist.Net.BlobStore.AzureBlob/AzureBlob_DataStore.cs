﻿using Azure.Storage.Blobs;

namespace PolyPersist.Net.BlobStore.AzureBlob
{
    internal class AzureBlob_DataStore : IDataStore
    {
        private string _storeName;
        private AzureBlob_Connection _connection;
        internal BlobServiceClient _blobServiceClient;

        public AzureBlob_DataStore(string storeName, AzureBlob_Connection connection)
        {
            _storeName = storeName;
            _connection = connection;
            _blobServiceClient = connection._blobServiceClient;
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
        async Task<ICollection> IDataStore.CreateCollection<TFile>(string collectionName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, collectionName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == true)
                throw new Exception($"Collection '{collectionName}' is already exist in AzureBlob storage '{_storeName}'");

            await containerClient.CreateAsync().ConfigureAwait(false);

            ICollection collection = Activator
                .CreateInstance(typeof(AzureBlob_Container<>)
                .MakeGenericType(typeof(TFile)), containerClient) as ICollection;

            return collection;
        }

        /// <inheritdoc/>
        async Task<ICollection> IDataStore.GetCollectionByName<TFile>(string collectionName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, collectionName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Collection '{collectionName}' does not exist in AzureBlob storage '{_storeName}'");

            ICollection collection = Activator
                .CreateInstance(typeof(AzureBlob_Container<>)
                .MakeGenericType(typeof(TFile)), containerClient) as ICollection;

            return collection;
        }

        /// <inheritdoc/>
        async Task IDataStore.DropCollection(string collectionName)
        {
            BlobContainerClient containerClient = _blobServiceClient.GetBlobContainerClient(_containerName(_storeName, collectionName));
            if (await containerClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Collection '{collectionName}' does not exist in AzureBlob storage '{_storeName}'");

            await containerClient.DeleteAsync().ConfigureAwait(false);
        }

        private string _containerName(string storeName, string collectionName)
            => $"{storeName}.{collectionName}";
    }
}
