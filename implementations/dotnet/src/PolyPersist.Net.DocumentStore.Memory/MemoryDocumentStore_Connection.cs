namespace PolyPersist.Net.DocumentStore.Memory
{
    internal class MemoryDocumentDB_Connection : IConnection
    {
        private List<IDataStore> _DataStores = [];

        MemoryDocumentDB_Connection()
        {
        }

        /// <inheritdoc/>
        Task<bool> IConnection.IsDataStoreExists(string storeName)
        {
            if (_DataStores.FindIndex(d => d.Name == storeName) != -1)
                return Task.FromResult(true);
            else
                return Task.FromResult(false);
        }

        /// <inheritdoc/>
        Task<IDataStore> IConnection.GetDataStoreByName(string storeName)
        {
            IDataStore dataStore = _DataStores.Find(d => d.Name == storeName);
            if (dataStore == null)
                throw new Exception($"DataStore: {storeName} does not exists");

            return Task.FromResult( dataStore );
        }

        /// <inheritdoc/>
        Task<IDataStore> IConnection.CreateDataStore(string storeName)
        {
            IDataStore dataStore = _DataStores.Find(d => d.Name == storeName);
            if (dataStore != null)
                throw new Exception($"DataStore: {storeName} is already exists");

            dataStore = new MemoryDocumentDB_DataStore(storeName, this);
            _DataStores.Add(dataStore);

            return Task.FromResult(dataStore);
        }

        /// <inheritdoc/>
        Task IConnection.DropDataStore(IDataStore dataStore)
        {
            if (_DataStores.FindIndex(d => d.Name == dataStore.Name) == -1)
                throw new Exception($"DataStore: {dataStore.Name} does not exists");

            _DataStores.Remove(dataStore);
            return Task.CompletedTask;
        }
    }
}
