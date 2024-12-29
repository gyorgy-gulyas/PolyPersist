﻿namespace PolyPersist.Net.BlobStore.Memory
{
    internal class MemoryBlobStore_DataStore : IDataStore
    {
        internal string _storeName;
        internal MemoryBlobStore_Connection _connection;
        internal List<_CollectionData> _Collections = [];

        public MemoryBlobStore_DataStore(string storeName, MemoryBlobStore_Connection connection)
        {
            _storeName = storeName;
            _connection = connection;
        }

        /// <inheritdoc/>
        IConnection IDataStore.Connection => _connection;
        /// <inheritdoc/>
        IDataStore.StorageModels IDataStore.StorageModel => IDataStore.StorageModels.Document;
        /// <inheritdoc/>
        string IDataStore.ProviderName => "Memory_BlobStore";
        /// <inheritdoc/>
        string IDataStore.Name => _storeName;

        /// <inheritdoc/>
        Task<bool> IDataStore.IsCollectionExists(string collectionName)
        {
            if (_Collections.FindIndex(c => c.Name == collectionName) != -1)
                return Task.FromResult(true);
            else
                return Task.FromResult(false);
        }

        /// <inheritdoc/>
        Task<ICollection<TEntity>> IDataStore.GetCollectionByName<TEntity>(string collectionName)
        {
            _CollectionData collectionData = _Collections.Find(c => c.Name == collectionName);
            if (collectionData == null)
                throw new Exception($"Collection '{collectionName}' does not exist in Mongo Database '{_storeName}'");

            ICollection<TEntity> collection = new MemoryBlobStore_Collection<TEntity>(collectionName, collectionData, this);
            return Task.FromResult(collection);
        }

        /// <inheritdoc/>
        Task<ICollection<TEntity>> IDataStore.CreateCollection<TEntity>(string collectionName)
        {
            _CollectionData collectionData = new(collectionName);
            _Collections.Add(collectionData);

            ICollection<TEntity> collection = new MemoryBlobStore_Collection<TEntity>(collectionName, collectionData, this);
            return Task.FromResult(collection);
        }

        /// <inheritdoc/>
        Task IDataStore.DropCollection(string collectionName)
        {
            _CollectionData collectionData = _Collections.Find(c => c.Name == collectionName);
            if (collectionData == null)
                throw new Exception($"Collection '{collectionName}' does not exist in Mongo Database '{_storeName}'");

            _Collections.Remove(collectionData);
            return Task.CompletedTask;
        }
    }
    public class _CollectionData
    {
        internal _CollectionData(string name)
        {
            Name = name;
        }

        internal string Name;
        internal Dictionary<(string id, string pk), _BlobData> MapOfBlobs = [];
        internal List<_BlobData> ListOfBlobs = [];
    }

    public class _BlobData
    {
        internal string id;
        internal string partionKey;
        internal string etag;
        internal string MetadataJSON;
        internal byte[] Content;
    }
}