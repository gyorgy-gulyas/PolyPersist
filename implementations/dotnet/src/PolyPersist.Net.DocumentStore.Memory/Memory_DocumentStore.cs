﻿namespace PolyPersist.Net.DocumentStore.Memory
{
    internal class Memory_DocumentStore : IDocumentStore
    {
        internal string _storeName;
        internal List<_CollectionData> _Collections = [];

        public Memory_DocumentStore(string storeName )
        {
            _storeName = storeName;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Document;
        /// <inheritdoc/>
        string IStore.ProviderName => "Memory_DocumentStore";
        /// <inheritdoc/>
        string IStore.Name => _storeName;

        /// <inheritdoc/>
        Task<bool> IDocumentStore.IsCollectionExists(string collectionName)
        {
            if (_Collections.FindIndex(c => c.Name == collectionName) != -1)
                return Task.FromResult(true);
            else
                return Task.FromResult(false);
        }

        /// <inheritdoc/>
        Task<IDocumentCollection<TDocument>> IDocumentStore.GetCollectionByName<TDocument>(string collectionName)
        {
            _CollectionData collectionData = _Collections.Find(c => c.Name == collectionName);
            if (collectionData == null)
                throw new Exception($"Collection '{collectionName}' does not exist in Mongo Database '{_storeName}'");

            IDocumentCollection<TDocument> collection = new Memory_DocumentCollection<TDocument>( collectionName, collectionData, this);
            return Task.FromResult(collection);
        }

        /// <inheritdoc/>
        Task<IDocumentCollection<TDocument>> IDocumentStore.CreateCollection<TDocument>(string collectionName)
        {
            _CollectionData collectionData = new(collectionName);
            _Collections.Add(collectionData);

            IDocumentCollection<TDocument> collection = new Memory_DocumentCollection<TDocument>(collectionName, collectionData, this);
            return Task.FromResult(collection);
        }

        /// <inheritdoc/>
        Task IDocumentStore.DropCollection(string collectionName)
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
        internal Dictionary<(string id, string pk), _RowData> MapOfDocments = [];
        internal List<_RowData> ListOfDocments = [];
        internal List<(string name, string[] keys)> Indexes = [];
    }

    public class _RowData
    {
        internal string id;
        internal string partionKey;
        internal string etag;
        internal string Value;
    }
}
