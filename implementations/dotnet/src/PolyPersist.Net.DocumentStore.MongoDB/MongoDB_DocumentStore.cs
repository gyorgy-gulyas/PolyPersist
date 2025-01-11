using MongoDB.Driver;

namespace PolyPersist.Net.DocumentStore.MongoDB
{
    internal class MongoDB_DocumentStore : IDocumentStore
    {
        internal readonly IMongoDatabase _mongoDatabase;

        public MongoDB_DocumentStore(IMongoDatabase mongoDatabase)
        {
            _mongoDatabase = mongoDatabase;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Document;
        /// <inheritdoc/>
        string IStore.ProviderName => "MongoDB";
        /// <inheritdoc/>
        string IStore.Name => _mongoDatabase.DatabaseNamespace.DatabaseName;

        /// <inheritdoc/>
        async Task<bool> IDocumentStore.IsCollectionExists(string collectionName)
        {
            IAsyncCursor<string> collectionCursor = await _mongoDatabase.ListCollectionNamesAsync();
            collectionCursor.MoveNext();

            IEnumerable<string> collections = collectionCursor.Current;
            return collections.Contains(collectionName);
        }

        /// <inheritdoc/>
        async Task<IDocumentCollection<TEntity>> IDocumentStore.GetCollectionByName<TEntity>(string collectionName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IDocumentStore).IsCollectionExists(collectionName).ConfigureAwait(false) == false)
                return null;

            IMongoCollection<TEntity> mongoCollection = _mongoDatabase.GetCollection<TEntity>(collectionName);

            return new MongoDB_DocumentCollection<TEntity>(mongoCollection, this);
        }

        /// <inheritdoc/>
        async Task<IDocumentCollection<TEntity>> IDocumentStore.CreateCollection<TEntity>(string collectionName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IDocumentStore).IsCollectionExists(collectionName).ConfigureAwait(false) == true)
                throw new Exception($"Collection '{collectionName}' is already exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            await _mongoDatabase.CreateCollectionAsync(collectionName).ConfigureAwait(false);
            IMongoCollection<TEntity> mongoCollection = _mongoDatabase.GetCollection<TEntity>(collectionName);

            return new MongoDB_DocumentCollection<TEntity>(mongoCollection, this);
        }

        /// <inheritdoc/>
        async Task IDocumentStore.DropCollection(string collectionName)
        {
            if (await (this as IDocumentStore).IsCollectionExists(collectionName).ConfigureAwait(false) == false)
                throw new Exception($"Collection '{collectionName}' does not exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            await _mongoDatabase.DropCollectionAsync(collectionName).ConfigureAwait(false);
        }
    }
}
