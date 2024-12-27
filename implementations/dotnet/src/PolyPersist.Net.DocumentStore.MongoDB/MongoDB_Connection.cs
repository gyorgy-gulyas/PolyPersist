using MongoDB.Driver;


namespace PolyPersist.Net.DocumentStore.MongoDB
{
    /// <inheritdoc/>
    internal class MongoDB_Connection : IConnection
    {
        private string _connectionString;
        private MongoClient _mongoClient;

        public MongoDB_Connection(string connectionString)
        {
            _connectionString = connectionString;
            _mongoClient = new MongoClient(connectionString);
        }

        /// <inheritdoc/>
        async Task<bool> IConnection.IsDataStoreExists(string storeName)
        {
            IAsyncCursor<string> databasesCusrsor = await _mongoClient.ListDatabaseNamesAsync();
            databasesCusrsor.MoveNext();

            IEnumerable<string> databases = databasesCusrsor.Current;
            return databases.Contains(storeName);
        }

        /// <inheritdoc/>
        async Task<IDataStore> IConnection.GetDataStoreByName(string storeName)
        {
            if (await (this as IConnection).IsDataStoreExists(storeName) == false)
                throw new Exception($"DataStore: {storeName} does not exists");

            IMongoDatabase mondoDatabase = _mongoClient.GetDatabase(storeName);
            return new MongoDB_Database(mondoDatabase, this);
        }

        /// <inheritdoc/>
        async Task<IDataStore> IConnection.CreateDataStore(string storeName)
        {
            if (await (this as IConnection).IsDataStoreExists(storeName) == false)
                throw new Exception($"DataStore: {storeName} is already exists");

            IMongoDatabase mondoDatabase = _mongoClient.GetDatabase(storeName);
            await mondoDatabase.CreateCollectionAsync("__system");

            return new MongoDB_Database(mondoDatabase, this);
        }

        /// <inheritdoc/>
        async Task IConnection.DropDataStore(IDataStore dataStore)
        {
            if (await (this as IConnection).IsDataStoreExists(dataStore.Name) == false)
                throw new Exception($"DataStore: {dataStore.Name} does not exists");

            await _mongoClient.DropDatabaseAsync(dataStore.Name);
        }
    }
}
