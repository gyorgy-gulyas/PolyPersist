using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.FileSystem
{
    internal class FileSystem_BlobContainer<TBlob> : IBlobContainer<TBlob> where TBlob : IBlob, new()
    {
        private readonly string _containerPath;

        public FileSystem_BlobContainer(string containerPath)
        {
            _containerPath = containerPath;
            Directory.CreateDirectory(_containerPath);
        }

        string IBlobContainer<TBlob>.Name => new DirectoryInfo(_containerPath).Name;

        Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            var path = _makeFilePath(blob.PartitionKey, blob.id);
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            using var fs = File.Create(path);
            content.CopyTo(fs);
            File.WriteAllText(path + ".meta.json", JsonSerializer.Serialize(MetadataHelper.GetMetadata(blob)));
            return Task.CompletedTask;
        }

        Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            var path = _makeFilePath(blob.PartitionKey, blob.id);
            var fs = File.OpenRead(path);
            return Task.FromResult<Stream>(fs);
        }

        Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            var path = _makeFilePath(partitionKey, id);
            if (!File.Exists(path)) 
                return Task.FromResult<TBlob>( default );

            var blob = new TBlob
            {
                id = id,
                PartitionKey = partitionKey
            };

            var metadataPath = path + ".meta.json";
            if (File.Exists(metadataPath))
            {
                var json = File.ReadAllText(metadataPath);
                var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                MetadataHelper.SetMetadata(blob, dict);
            }

            return Task.FromResult(blob);
        }

        Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            var path = _makeFilePath(partitionKey, id);
            if (!File.Exists(path)) throw new FileNotFoundException("Blob not found", path);
            File.Delete(path);
            var metaPath = path + ".meta.json";
            if (File.Exists(metaPath)) File.Delete(metaPath);
            return Task.CompletedTask;
        }

        Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            return Upload(blob, content);
        }

        Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            var path = _makeFilePath(blob.PartitionKey, blob.id);
            if (!File.Exists(path)) throw new FileNotFoundException("Blob not found", path);
            File.WriteAllText(path + ".meta.json", JsonSerializer.Serialize(MetadataHelper.GetMetadata(blob)));
            return Task.CompletedTask;
        }

        object IBlobContainer<TBlob>.GetUnderlyingImplementation() => _containerPath;

        private string _makeFilePath(string partitionKey, string id)
        {
            return Path.Combine(_containerPath, partitionKey, id);
        }
    }
}