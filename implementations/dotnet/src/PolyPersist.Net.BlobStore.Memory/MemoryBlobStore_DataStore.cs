namespace PolyPersist.Net.BlobStore.Memory
{
    internal class MemoryBlobStore_DataStore : IBlobStore
    {
        internal string _storeName;
        internal List<_ContainerData> _Containers = [];

        public MemoryBlobStore_DataStore(string storeName)
        {
            _storeName = storeName;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Document;
        /// <inheritdoc/>
        string IStore.ProviderName => "Memory_BlobStore";
        /// <inheritdoc/>
        string IStore.Name => _storeName;

        /// <inheritdoc/>
        Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            if (_Containers.FindIndex(c => c.Name == containerName) != -1)
                return Task.FromResult(true);
            else
                return Task.FromResult(false);
        }

        /// <inheritdoc/>
        Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            _ContainerData containerData = _Containers.Find(c => c.Name == containerName);
            if (containerData == null)
                throw new Exception($"Container '{containerName}' does not exist in Mongo Database '{_storeName}'");

            IBlobContainer<TBlob> container = new MemoryBlobStore_Container<TBlob>(containerName, containerData, this);
            return Task.FromResult(container);
        }

        /// <inheritdoc/>
        Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            _ContainerData containerData = new(containerName);
            _Containers.Add(containerData);

            IBlobContainer<TBlob> container = new MemoryBlobStore_Container<TBlob>(containerName, containerData, this);
            return Task.FromResult(container);
        }

        /// <inheritdoc/>
        Task IBlobStore.DropContainer(string containerName)
        {
            _ContainerData containerData = _Containers.Find(c => c.Name == containerName);
            if (containerData == null)
                throw new Exception($"Container '{containerName}' does not exist in Mongo Database '{_storeName}'");

            _Containers.Remove(containerData);
            return Task.CompletedTask;
        }
    }
    public class _ContainerData
    {
        internal _ContainerData(string name)
        {
            Name = name;
        }

        internal string Name;
        internal Dictionary<(string id, string pk), _BlobData> MapOfBlobs = [];
        internal List<_BlobData> ListOfBlobs = [];
    }

    public class _BlobData
    {
        internal string id;
        internal string partionKey;
        internal string etag;
        internal string MetadataJSON;
        internal byte[] Content;
    }
}
