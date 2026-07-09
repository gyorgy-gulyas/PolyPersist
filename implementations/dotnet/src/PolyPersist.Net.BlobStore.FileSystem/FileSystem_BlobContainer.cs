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

            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            using var fs = File.Create(path);
            content.CopyTo(fs);
            File.WriteAllText(path + ".meta.json", BlobMetadata.Serialize(blob));

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
            var metadataPath = path + ".meta.json";
            if (File.Exists(path) == false || File.Exists(metadataPath) == false)
                return Task.FromResult<TBlob>(default!);

            var blob = BlobMetadata.Deserialize<TBlob>(File.ReadAllText(metadataPath));
            // the blob is identified by (partitionKey, id): a different partition is not it
            if (blob.PartitionKey != partitionKey)
                return Task.FromResult<TBlob>(default!);

            return Task.FromResult(blob);
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            var path = _makeFilePath(id);
            var metaPath = path + ".meta.json";
            if (File.Exists(path) == false || File.Exists(metaPath) == false
                || BlobMetadata.Deserialize<TBlob>(File.ReadAllText(metaPath)).PartitionKey != partitionKey)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be removed because it is does not exist");

            File.Delete(path);
            File.Delete(metaPath);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            var path = _makeFilePath(blob.id);
            var metaPath = path + ".meta.json";
            if (File.Exists(path) == false || File.Exists(metaPath) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            // optimistic concurrency: the stored etag must still match
            CollectionCommon.CheckEtagMatch(BlobMetadata.Deserialize<TBlob>(File.ReadAllText(metaPath)), blob);

            // write to a temp file then replace atomically, so a failed write does not truncate
            // (lose) the existing content
            var tempPath = path + ".tmp";
            using (var fs = File.Create(tempPath))
                content.CopyTo(fs);
            File.Move(tempPath, path, overwrite: true);

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            File.WriteAllText(metaPath, BlobMetadata.Serialize(blob));

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            var path = _makeFilePath(blob.id);
            var metaPath = path + ".meta.json";
            if (File.Exists(path) == false || File.Exists(metaPath) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            CollectionCommon.CheckEtagMatch(BlobMetadata.Deserialize<TBlob>(File.ReadAllText(metaPath)), blob);

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;
            File.WriteAllText(metaPath, BlobMetadata.Serialize(blob));

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation() => _containerPath;

        private string _makeFilePath(string id)
        {
            // Resolve the path and make sure it stays inside the container: an id such as
            // "../../etc/passwd" must not escape (path traversal). Hierarchical ids
            // ("folder/blob") are still allowed as long as they resolve within the root.
            string root = Path.GetFullPath(_containerPath);
            string full = Path.GetFullPath(Path.Combine(root, id));
            string rootWithSeparator = root.EndsWith(Path.DirectorySeparatorChar)
                ? root
                : root + Path.DirectorySeparatorChar;
            if (full.StartsWith(rootWithSeparator, StringComparison.Ordinal) == false)
                throw new ArgumentException($"Invalid blob id '{id}': it resolves outside the container.");
            return full;
        }
    }
}
