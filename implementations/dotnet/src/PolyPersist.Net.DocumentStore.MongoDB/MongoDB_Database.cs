using MongoDB.Driver;

namespace PolyPersist.Net.DocumentStore.MongoDB
{
    internal class MongoDB_Database : IDataStore
    {
        internal readonly IMongoDatabase _mongoDatabase;
        internal readonly MongoDB_Connection _connection;

        public MongoDB_Database(IMongoDatabase mongoDatabase, MongoDB_Connection connection)
        {
            _mongoDatabase = mongoDatabase;
            _connection = connection;
        }

        /// <inheritdoc/>
        IConnection IDataStore.Connection => _connection;
        /// <inheritdoc/>
        IDataStore.StorageModels IDataStore.StorageModel => IDataStore.StorageModels.Document;
        /// <inheritdoc/>
        string IDataStore.ProviderName => "MongoDB";
        /// <inheritdoc/>
        string IDataStore.Name => _mongoDatabase.DatabaseNamespace.DatabaseName;

        /// <inheritdoc/>
        async Task<bool> IDataStore.IsCollectionExists(string collectionName)
        {
            IAsyncCursor<string> collectionCursor = await _mongoDatabase.ListCollectionNamesAsync();
            collectionCursor.MoveNext();

            IEnumerable<string> collections = collectionCursor.Current;
            return collections.Contains(collectionName);
        }

        /// <inheritdoc/>
        async Task<ICollection> IDataStore.GetCollectionByName<TEntity>(string collectionName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IDataStore).IsCollectionExists(collectionName).ConfigureAwait(false) == false)
                return null;

            IMongoCollection<TEntity> mongoCollection = _mongoDatabase.GetCollection<TEntity>(collectionName);

            ICollection collection = Activator
                .CreateInstance(typeof(MongoDB_Collection<>)
                .MakeGenericType(typeof(TEntity)), mongoCollection, this ) as ICollection;
            return collection;
        }

        /// <inheritdoc/>
        async Task<ICollection> IDataStore.CreateCollection<TEntity>(string collectionName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));

            if (await (this as IDataStore).IsCollectionExists(collectionName).ConfigureAwait(false) == true)
                throw new Exception($"Collection '{collectionName}' is already exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            await _mongoDatabase.CreateCollectionAsync(collectionName).ConfigureAwait(false);
            IMongoCollection<TEntity> mongoCollection = _mongoDatabase.GetCollection<TEntity>(collectionName);

            ICollection collection = Activator
                .CreateInstance(typeof(MongoDB_Collection<>)
                .MakeGenericType(typeof(TEntity)), mongoCollection, this) as ICollection;
            return collection;
        }

        /// <inheritdoc/>
        async Task IDataStore.DropCollection(string collectionName)
        {
            if (await (this as IDataStore).IsCollectionExists(collectionName).ConfigureAwait(false) == false)
                throw new Exception($"Collection '{collectionName}' does not exist in Mongo Database '{_mongoDatabase.DatabaseNamespace.DatabaseName}'");

            await _mongoDatabase.DropCollectionAsync(collectionName).ConfigureAwait(false);
        }
    }
}
