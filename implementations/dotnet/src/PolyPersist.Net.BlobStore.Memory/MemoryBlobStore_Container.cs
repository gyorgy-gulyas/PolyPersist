using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.BlobStore.Memory
{
    internal class MemoryBlobStore_Container<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob
    {
        internal string _name;
        internal _ContainerData _collectionData;
        internal MemoryBlobStore_DataStore _dataStore;

        internal MemoryBlobStore_Container(string name, _ContainerData collectionData, MemoryBlobStore_DataStore dataStore)
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
            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            blob.etag = Guid.NewGuid().ToString();
            if (string.IsNullOrEmpty(blob.id) == true)
                blob.id = Guid.NewGuid().ToString();

            _BlobData blobData = new()
            {
                id = blob.id,
                partionKey = blob.PartitionKey,
                etag = blob.etag,
                MetadataJSON = JsonSerializer.Serialize(blob, typeof(TBlob), JsonOptionsProvider.Options),
                Content = _streamToByteArray(content)
            };

            _collectionData.MapOfBlobs.Add((blob.id, blob.PartitionKey), blobData);
            _collectionData.ListOfBlobs.Add(blobData);
        }

        /// <inheritdoc/>
        Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((blob.id, blob.PartitionKey), out _BlobData blobData) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist");

            using MemoryStream destination = new();
            destination.Write(blobData.Content, 0, blobData.Content.Length);
            return Task.FromResult<Stream>(destination);
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((id, partitionKey), out _BlobData blobData) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be removed because it is does not exist");

            _collectionData.MapOfBlobs.Remove((id, partitionKey));
            _collectionData.ListOfBlobs.Remove(blobData);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((id, partitionKey), out _BlobData blobData) == true)
            {
                TBlob blob = JsonSerializer.Deserialize<TBlob>(blobData.MetadataJSON, JsonOptionsProvider.Options);
                return Task.FromResult(blob);
            }

            return Task.FromResult(default(TBlob));
        }

        /// <inheritdoc/>
        Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            if (_collectionData.MapOfBlobs.TryGetValue((blob.id, blob.PartitionKey), out _BlobData blobData) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            blobData.Content = _streamToByteArray(content);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            await CollectionCommon.CheckBeforeUpdate(blob).ConfigureAwait(false);

            if (_collectionData.MapOfBlobs.TryGetValue((blob.id, blob.PartitionKey), out _BlobData blobData) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be removed because it is does dot exist");

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
