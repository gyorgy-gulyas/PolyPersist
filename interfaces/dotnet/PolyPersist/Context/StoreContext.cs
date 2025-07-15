namespace PolyPersist.Net.Context
{
    public abstract class StoreContext
    {
        private readonly IStoreProvider _storeProvider;
        private readonly Dictionary<Type,string> _collectionsByType = [];

        public StoreContext(IStoreProvider storeProvider)
        {
            _storeProvider = storeProvider;
        }

        public async Task<IDocumentCollection<TDocument>> GetOrCreateDocumentCollection<TDocument>(string collectionName = default(string))
            where TDocument : IDocument, new()
        {
            IDocumentStore documentStore = (IDocumentStore)_storeProvider.getStore(IStore.StorageModels.Document);

            if (string.IsNullOrEmpty(collectionName) == true)
                collectionName = typeof(TDocument).Name;

            var collection = await documentStore.GetCollectionByName<TDocument>(collectionName);
            if (collection == null)
                collection = await documentStore.CreateCollection<TDocument>(collectionName);

            _collectionsByType[typeof(TDocument)] = collectionName;

            return collection;
        }

        public async Task<IColumnTable<TRow>> GetOrCreateColumnTable<TRow>(string tableName = default(string))
            where TRow : IRow, new()
        {
            IColumnStore columnStore = (IColumnStore)_storeProvider.getStore(IStore.StorageModels.ColumnStore);

            if (string.IsNullOrEmpty(tableName) == true)
                tableName = typeof(TRow).Name;

            var table = await columnStore.GetTableByName<TRow>(tableName);
            if (table == null)
                table = await columnStore.CreateTable<TRow>(tableName);

            _collectionsByType[typeof(TRow)] = tableName;

            return table;
        }
        
        public async Task<IBlobContainer<TBlob>> GetOrCreateBlobContainer<TBlob>( string containerName )
            where TBlob : IBlob, new()
        {
            IBlobStore blobStore = (IBlobStore)_storeProvider.getStore(IStore.StorageModels.BlobStore);

            if (string.IsNullOrEmpty(containerName) == true)
                containerName = typeof(TBlob).Name;

            var container = await blobStore.GetContainerByName<TBlob>(containerName);
            if (container == null)
                container = await blobStore.CreateContainer<TBlob>(containerName);

            _collectionsByType[typeof(TBlob)] = containerName;

            return container;
        }
    }
}
