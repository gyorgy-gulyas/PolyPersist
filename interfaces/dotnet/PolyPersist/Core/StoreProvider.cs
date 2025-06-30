namespace PolyPersist.Net.Core
{
    public class StoreProvider : IStoreProvider
    {
        IStore IStoreProvider.getStore(IStore.StorageModels storageModel )
        {
            return storageModel switch
            {
                IStore.StorageModels.Relational => GetRelationalStore(),
                IStore.StorageModels.Document => GetDocumentStore(),
                IStore.StorageModels.ColumnStore => GetColumnStore(),
                IStore.StorageModels.BlobStore => GetBlobStore(),
                _ => throw new NotImplementedException(),
            };
        }

        protected virtual IStore GetRelationalStore() => throw new NotImplementedException();
        protected virtual IDocumentStore GetDocumentStore() => throw new NotImplementedException();
        protected virtual IColumnStore GetColumnStore() => throw new NotImplementedException();
        protected virtual IBlobStore GetBlobStore() => throw new NotImplementedException();
    }
}
