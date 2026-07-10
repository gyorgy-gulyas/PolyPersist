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
                IStore.StorageModels.EventStore => GetEventStore(),
                IStore.StorageModels.Analytical => GetAnalyticalStore(),
                IStore.StorageModels.Search => GetSearchStore(),
                IStore.StorageModels.Cache => GetCacheStore(),
                _ => throw new NotImplementedException(),
            };
        }

        protected virtual IStore GetRelationalStore() => throw new NotImplementedException();
        protected virtual IDocumentStore GetDocumentStore() => throw new NotImplementedException();
        protected virtual IColumnStore GetColumnStore() => throw new NotImplementedException();
        protected virtual IBlobStore GetBlobStore() => throw new NotImplementedException();
        protected virtual IEventStore GetEventStore() => throw new NotImplementedException();
        protected virtual IAnalyticalStore GetAnalyticalStore() => throw new NotImplementedException();
        protected virtual ISearchStore GetSearchStore() => throw new NotImplementedException();
        protected virtual ICacheStore GetCacheStore() => throw new NotImplementedException();
    }
}
