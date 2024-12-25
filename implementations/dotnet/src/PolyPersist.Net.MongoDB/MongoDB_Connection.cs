using MongoDB.Driver;
using System.Security.Cryptography.X509Certificates;


namespace PolyPersist.Net.MongoDB
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
                return null;

            IMongoDatabase mondoDatabase = _mongoClient.GetDatabase(storeName);
            return new MongoDB_Database(mondoDatabase, this);
        }

        /// <inheritdoc/>
        async Task<IDataStore> IConnection.CreateDataStore(string storeName)
        {
            if (await (this as IConnection).IsDataStoreExists(storeName) == false)
                return await (this as IConnection).GetDataStoreByName(storeName);

            IMongoDatabase mondoDatabase = _mongoClient.GetDatabase(storeName);
            await mondoDatabase.CreateCollectionAsync("__system");

            return new MongoDB_Database(mondoDatabase, this);
        }

        /// <inheritdoc/>
        async Task<bool> IConnection.DropDataStore(IDataStore dataStore)
        {
            if (await (this as IConnection).IsDataStoreExists(dataStore.Name) == false)
                return false;

            await _mongoClient.DropDatabaseAsync(dataStore.Name);
            return true;
        }
    }
}
