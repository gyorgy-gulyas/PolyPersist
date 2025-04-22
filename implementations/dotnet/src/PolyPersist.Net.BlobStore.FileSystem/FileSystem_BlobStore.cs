using PolyPersist;

namespace PolyPersist.Net.BlobStore.FileSystem
{
    internal class FileSystemBlobStore : IBlobStore
    {
        private readonly string _rootPath;
        private readonly string _storeName;

        public FileSystemBlobStore(string storeName, string basePath)
        {
            _storeName = storeName;
            _rootPath = Path.Combine(basePath, storeName);
            Directory.CreateDirectory(_rootPath);
        }

        public IStore.StorageModels StorageModel => IStore.StorageModels.BlobStore;
        public string ProviderName => "FileSystem";
        public string Name => _storeName;

        public Task<bool> IsContainerExists(string containerName)
        {
            var path = Path.Combine(_rootPath, containerName);
            return Task.FromResult(Directory.Exists(path));
        }

        public Task<IBlobContainer<TBlob>> CreateContainer<TBlob>(string containerName) where TBlob : IBlob, new()
        {
            var path = Path.Combine(_rootPath, containerName);
            if (Directory.Exists(path)) throw new Exception($"Container '{containerName}' already exists");
            Directory.CreateDirectory(path);
            return Task.FromResult<IBlobContainer<TBlob>>(new FileSystem_BlobContainer<TBlob>(path));
        }

        public Task<IBlobContainer<TBlob>> GetContainerByName<TBlob>(string containerName) where TBlob : IBlob, new()
        {
            var path = Path.Combine(_rootPath, containerName);
            if (!Directory.Exists(path)) throw new Exception($"Container '{containerName}' does not exist");
            return Task.FromResult<IBlobContainer<TBlob>>(new FileSystem_BlobContainer<TBlob>(path));
        }

        public Task DropContainer(string containerName)
        {
            var path = Path.Combine(_rootPath, containerName);
            if (!Directory.Exists(path)) throw new Exception($"Container '{containerName}' does not exist");
            Directory.Delete(path, recursive: true);
            return Task.CompletedTask;
        }
    }
}