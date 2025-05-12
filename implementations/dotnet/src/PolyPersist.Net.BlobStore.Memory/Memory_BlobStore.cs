namespace PolyPersist.Net.BlobStore.Memory
{
    public class Memory_BlobStore : IBlobStore
    {
        internal List<_ContainerData> _Containers = [];

        public Memory_BlobStore(string connectionString)
        {
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.BlobStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "Memory_BlobStore";

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
                throw new Exception($"Container '{containerName}' does not exist in Memory Blob Strore");

            IBlobContainer<TBlob> container = new Memory_BlobContainer<TBlob>(containerName, containerData, this);
            return Task.FromResult(container);
        }

        /// <inheritdoc/>
        Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            _ContainerData containerData = _Containers.Find(c => c.Name == containerName);
            if (containerData != null)
                throw new Exception($"Container '{containerName}' is already exist");

            containerData = new(containerName);
            _Containers.Add(containerData);

            IBlobContainer<TBlob> container = new Memory_BlobContainer<TBlob>(containerName, containerData, this);
            return Task.FromResult(container);
        }

        /// <inheritdoc/>
        Task IBlobStore.DropContainer(string containerName)
        {
            _ContainerData containerData = _Containers.Find(c => c.Name == containerName);
            if (containerData == null)
                throw new Exception($"Container '{containerName}' does not exist in Memory Blob Strore");

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
        internal string partitionKey;
        internal string etag;
        internal string MetadataJSON;
        internal byte[] Content;
    }
}
