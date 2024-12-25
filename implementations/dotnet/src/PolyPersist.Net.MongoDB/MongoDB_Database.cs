using MongoDB.Driver;

namespace PolyPersist.Net.MongoDB
{
    internal class MongoDB_Database : IDataStore
    {
        private readonly IMongoDatabase _mongoDatabase;
        private readonly MongoDB_Connection _connection;

        public MongoDB_Database(IMongoDatabase mongoDatabase, MongoDB_Connection connection ) 
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
        Task<ICollection<TEntity>> IDataStore.GetCollectionByName<TEntity>(string collectionName)
        {
            MongoDB_Serializer.RegisterType<TEntity>(typeof(TEntity));
            IMongoCollection<TEntity> mongoCollection = _mongoDatabase.GetCollection<TEntity>(collectionName);

            return Task.FromResult<ICollection<TEntity>>( new MongoDB_Collection<TEntity>(mongoCollection, this) );
        }

        /// <inheritdoc/>
        Task<ICollection<TEntity>> IDataStore.CreateCollection<TEntity>(string collectionName)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        Task<bool> IDataStore.DropCollection<TEntity>(ICollection<TEntity> collection)
        {
            throw new NotImplementedException();
        }
    }
}
