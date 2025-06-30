namespace PolyPersist.Net.Context
{
    public abstract class StoreContext
    {
        private readonly IStoreProvider _storeProvider;

        public StoreContext(IStoreProvider storeProvider)
        {
            _storeProvider = storeProvider;
        }
    }
}
