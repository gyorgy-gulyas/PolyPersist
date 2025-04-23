namespace PolyPersist.Net.BlobStore.FileSystem
{
    internal class FileSystemBlobStore : IBlobStore
    {
        private readonly string _rootPath;

        public FileSystemBlobStore(string basePath)
        {
            _rootPath = basePath;
            Directory.CreateDirectory(_rootPath);
        }

        public IStore.StorageModels StorageModel => IStore.StorageModels.BlobStore;
        string IStore.ProviderName => "FileSystem";

        Task<bool> IBlobStore.IsContainerExists(string containerName)
        {
            var path = Path.Combine(_rootPath, containerName);
            return Task.FromResult(Directory.Exists(path));
        }

        Task<IBlobContainer<TBlob>> IBlobStore.CreateContainer<TBlob>(string containerName)
        {
            var path = Path.Combine(_rootPath, containerName);
            if (Directory.Exists(path)) throw new Exception($"Container '{containerName}' already exists");
            Directory.CreateDirectory(path);
            return Task.FromResult<IBlobContainer<TBlob>>(new FileSystem_BlobContainer<TBlob>(path));
        }

        Task<IBlobContainer<TBlob>> IBlobStore.GetContainerByName<TBlob>(string containerName)
        {
            var path = Path.Combine(_rootPath, containerName);
            if (!Directory.Exists(path)) throw new Exception($"Container '{containerName}' does not exist");
            return Task.FromResult<IBlobContainer<TBlob>>(new FileSystem_BlobContainer<TBlob>(path));
        }

        Task IBlobStore.DropContainer(string containerName)
        {
            var path = Path.Combine(_rootPath, containerName);
            if (!Directory.Exists(path)) throw new Exception($"Container '{containerName}' does not exist");
            Directory.Delete(path, recursive: true);
            return Task.CompletedTask;
        }
    }
}