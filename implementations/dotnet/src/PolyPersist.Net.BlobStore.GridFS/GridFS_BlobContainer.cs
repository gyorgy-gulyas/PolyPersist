using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using PolyPersist.Net.Common;
using System.Text;

namespace PolyPersist.Net.BlobStore.GridFS
{
    internal class GridFS_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private GridFSBucket _gridFSBucket;
        private GridFS_BlobStore _store;
        public IMongoCollection<GridFSFileInfo> _filesCollection;
        public IMongoCollection<TBlob> _metadataCollection;

        public GridFS_BlobContainer(GridFSBucket gridFSBucket, GridFS_BlobStore store)
        {
            _gridFSBucket = gridFSBucket;
            _store = store;
            _filesCollection = _store._mongoDatabase.GetCollection<GridFSFileInfo>(gridFSBucket.Options.BucketName + ".files");
            _metadataCollection = _store._mongoDatabase.GetCollection<TBlob>(gridFSBucket.Options.BucketName + ".metadata");
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _gridFSBucket.Options.BucketName;
        /// <inheritdoc/>
        IStore IBlobContainer<TBlob>.ParentStore => _store;

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            if (content == null || content.CanRead == false)
                throw new InvalidRequestException($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            CollectionCommon.CheckBeforeInsert(blob);

            if (string.IsNullOrEmpty(blob.id) == true)
                blob.id = Guid.NewGuid().ToString();
            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            try
            {
                // insert metadata
                await _metadataCollection.InsertOneAsync(blob).ConfigureAwait(false);
            }
            catch (MongoWriteException ex)
            {
                throw new DuplicateKeyException($"Blob '{typeof(TBlob).Name}' {blob.id} cannot be uploaded, beacuse of duplicate key", ex);
            }

            // Upload empty content to GridFS, to create a entity is database
            await _gridFSBucket.UploadFromStreamAsync(_makeId(blob), content).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            GridFSFileInfo fileInfo = await _getFileInfo(blob.PartitionKey, blob.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist");

            return await _gridFSBucket.OpenDownloadStreamAsync(fileInfo.Id);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            // Delete the GridFS content (chunks) BEFORE the metadata document. If the metadata
            // were deleted first and the chunk delete then failed (or the file was missing), the
            // chunks would be orphaned (a silent space leak; no cross-collection transaction).
            // This order leaves at worst a dangling metadata row, which is detectable/retryable.
            GridFSFileInfo fileInfo = await _getFileInfo(partitionKey, id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {id} can not be removed because it is does not exist");

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);

            DeleteResult result = await _metadataCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new PolyPersistException($"Blob '{typeof(TBlob).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {id} can not be removed because it is does not exist");
        }

        /// <inheritdoc/>
        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            IAsyncCursor<TBlob> cursor = await _metadataCollection.FindAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            TBlob entity = await cursor.FirstOrDefaultAsync().ConfigureAwait(false);

            return entity;
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            if (content == null || content.CanRead == false)
                throw new InvalidRequestException($"Blob '{typeof(TBlob).Name}' {blob.id} content cannot be read");

            GridFSFileInfo fileInfo = await _getFileInfo(blob.PartitionKey, blob.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new NotFoundException($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            string oldETag = blob.etag;
            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            // Update the metadata FIRST: FindOneAndReplace is the atomic etag guard, so a stale
            // update is rejected before the chunks are touched (otherwise the content would be
            // replaced even though the update fails). (Also fixes a NullRef: `blob` used to be
            // reassigned to the null result and then dereferenced in the throw.)
            var replaced = await _metadataCollection.FindOneAndReplaceAsync(e => e.id == blob.id && e.PartitionKey == blob.PartitionKey && e.etag == oldETag, blob).ConfigureAwait(false);
            if (replaced == null)
                throw new ConcurrencyConflictException($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is already changed");

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);
            await _gridFSBucket.UploadFromStreamAsync(_makeId(blob), content).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            CollectionCommon.CheckBeforeUpdate(blob);

            GridFSFileInfo fileInfo = await _getFileInfo(blob.PartitionKey, blob.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new NotFoundException($"Entity '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            string oldETag = blob.etag;
            blob.etag = Guid.NewGuid().ToString();
            blob.LastUpdate = DateTime.UtcNow;

            var replaced = await _metadataCollection.FindOneAndReplaceAsync(e => e.id == blob.id && e.PartitionKey == blob.PartitionKey && e.etag == oldETag, blob).ConfigureAwait(false);
            if (replaced == null)
                throw new ConcurrencyConflictException($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it is already changed");
        }


        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _gridFSBucket;
        }

        private string _makeId(TBlob blob)
            => _makeId(blob.PartitionKey, blob.id);

        private string _makeId(string partitionKey, string id)
            => $"{partitionKey}-{id}";

        static string StringToHex(string input)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(input);
            StringBuilder hex = new StringBuilder(bytes.Length * 2);
            foreach (byte b in bytes)
            {
                hex.AppendFormat("{0:x2}", b); // kisbetűs hex; használd {0:X2} ha nagybetűs kell
            }
            return hex.ToString();
        }
        private async Task<GridFSFileInfo> _getFileInfo(string partitionKey, string id)
        {
            IAsyncCursor<GridFSFileInfo> cursor = await _filesCollection.FindAsync(fi => fi.Filename == _makeId(partitionKey, id)).ConfigureAwait(false);
            return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
        }
    }
}
