using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.Memory
{
    internal class Memory_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        internal string _name;
        internal _ContainerData _collectionData;
        internal Memory_BlobStore _dataStore;

        internal Memory_BlobContainer(string name, _ContainerData collectionData, Memory_BlobStore dataStore)
        {
            _name = name;
            _collectionData = collectionData;
            _dataStore = dataStore;
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _name;

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            if (string.IsNullOrEmpty(blob.id) == true)
                blob.id = Guid.NewGuid().ToString();
            else if (_collectionData.MapOfBlobs.ContainsKey(blob.id) == true)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} cannot be uploaded, beacuse of duplicate key");

            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            _BlobData blobData = new()
            {
                id = blob.id,
                partitionKey = blob.PartitionKey,
                etag = blob.etag,
                MetadataJSON = JsonSerializer.Serialize(blob, typeof(TBlob), JsonOptionsProvider.Options),
                Content = _streamToByteArray(content)
            };
            _collectionData.MapOfBlobs.Add(blob.id, blobData);
            _collectionData.ListOfBlobs.Add(blobData);
        }

        /// <inheritdoc/>
        Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            if (_collectionData.MapOfBlobs.TryGetValue(blob.id, out _BlobData blobData) == false || blobData.partitionKey != blob.PartitionKey)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist");


            MemoryStream destination = new();
            destination.Write(blobData.Content, 0, blobData.Content.Length);
            destination.Seek(0, SeekOrigin.Begin);
            return Task.FromResult<Stream>(destination);
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            if (_collectionData.MapOfBlobs.TryGetValue(id, out _BlobData blobData) == false || blobData.partitionKey != partitionKey )
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be removed because it is does not exist");

            _collectionData.MapOfBlobs.Remove(id);
            _collectionData.ListOfBlobs.Remove(blobData);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            if (_collectionData.MapOfBlobs.TryGetValue(id, out _BlobData blobData) == true && blobData.partitionKey == partitionKey)
            {
                TBlob blob = JsonSerializer.Deserialize<TBlob>(blobData.MetadataJSON, JsonOptionsProvider.Options);
                return Task.FromResult(blob);
            }

            return Task.FromResult(default(TBlob));
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            if (_collectionData.MapOfBlobs.TryGetValue(blob.id, out _BlobData blobData) == false || blobData.partitionKey != blob.PartitionKey)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            blobData.Content = _streamToByteArray(content);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            await CollectionCommon.CheckBeforeUpdate(blob).ConfigureAwait(false);

            if (_collectionData.MapOfBlobs.TryGetValue(blob.id, out _BlobData blobData) == false || blobData.partitionKey != blob.PartitionKey)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is does dot exist");

            if (blobData.etag != blob.etag)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is already changed");

            blob.etag = Guid.NewGuid().ToString();
            blobData.etag = blob.etag;
            blobData.MetadataJSON = JsonSerializer.Serialize(blob, typeof(TBlob), JsonOptionsProvider.Options);
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _collectionData;
        }

        public static byte[] _streamToByteArray(Stream input)
        {
            using MemoryStream ms = new MemoryStream();
            input.CopyTo(ms);
            return ms.ToArray();
        }
    }
}
