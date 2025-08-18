using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.FileSystem
{
    internal class FileSystem_BlobContainer<TBlob> : IBlobContainer<TBlob> where TBlob : IBlob, new()
    {
        private readonly string _containerPath;
        private readonly FileSystem_BlobStore _store;

        public FileSystem_BlobContainer(string containerPath,FileSystem_BlobStore store)
        {
            _containerPath = containerPath;
            Directory.CreateDirectory(_containerPath);
            _store = store;
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => new DirectoryInfo(_containerPath).Name;
        IStore IBlobContainer<TBlob>.ParentStore => _store;

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            CollectionCommon.CheckBeforeInsert(blob);

            if (string.IsNullOrEmpty(blob.id) == true)
                blob.id = Guid.NewGuid().ToString();
            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            var path = _makeFilePath(blob.id);
            if (File.Exists(path))
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} cannot be uploaded, beacuse of duplicate key");

            Directory.CreateDirectory(Path.GetDirectoryName(path));
            using var fs = File.Create(path);
            content.CopyTo(fs);
            File.WriteAllText(path + ".meta.json", JsonSerializer.Serialize(blob));

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            var path = _makeFilePath(blob.id);
            if (File.Exists(path) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist");

            var fs = File.OpenRead(path);
            return Task.FromResult<Stream>(fs);
        }

        /// <inheritdoc/>
        Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            var path = _makeFilePath(id);
            if (File.Exists(path) == false)
                return Task.FromResult<TBlob>(default);

            var metadataPath = path + ".meta.json";
            if (File.Exists(metadataPath) == false)
                return Task.FromResult<TBlob>(default);

            var json = File.ReadAllText(metadataPath);
            var blob = JsonSerializer.Deserialize<TBlob>(json);
            return Task.FromResult(blob);
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            var path = _makeFilePath(id);
            if (File.Exists(path) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be removed because it is does not exist");

            File.Delete(path);
            var metaPath = path + ".meta.json";
            if (File.Exists(metaPath) == true)
                File.Delete(metaPath);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            var path = _makeFilePath(blob.id);
            if (File.Exists(path) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            using var fs = File.Create(path);
            content.CopyTo(fs);

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            File.WriteAllText(path + ".meta.json", JsonSerializer.Serialize(blob));


            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            var path = _makeFilePath(blob.id);
            if (File.Exists(path) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            File.WriteAllText(path + ".meta.json", JsonSerializer.Serialize(blob));

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation() => _containerPath;

        private string _makeFilePath(string id)
        {
            return Path.Combine(_containerPath, id);
        }
    }
}