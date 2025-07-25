namespace PolyPersist.Net.Context
{
    public abstract class StoreContext
    {
        private readonly IStoreProvider _storeProvider;
        private readonly Dictionary<Type, string> _collectionsByType = [];

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

            IDocumentCollection<TDocument> collection;
            if (await documentStore.IsCollectionExists(collectionName) == true)
                collection = await documentStore.GetCollectionByName<TDocument>(collectionName);
            else
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

            IColumnTable<TRow> table;
            if (await columnStore.IsTableExists(tableName) == true)
                table = await columnStore.GetTableByName<TRow>(tableName);
            else
                table = await columnStore.CreateTable<TRow>(tableName);

            _collectionsByType[typeof(TRow)] = tableName;

            return table;
        }

        public async Task<IBlobContainer<TBlob>> GetOrCreateBlobContainer<TBlob>(string containerName)
            where TBlob : IBlob, new()
        {
            IBlobStore blobStore = (IBlobStore)_storeProvider.getStore(IStore.StorageModels.BlobStore);

            if (string.IsNullOrEmpty(containerName) == true)
                containerName = typeof(TBlob).Name;

            IBlobContainer<TBlob> container;
            if (await blobStore.IsContainerExists(containerName) == true)
                container = await blobStore.GetContainerByName<TBlob>(containerName);
            else
                container = await blobStore.CreateContainer<TBlob>(containerName);

            _collectionsByType[typeof(TBlob)] = containerName;

            return container;
        }
    }
}
