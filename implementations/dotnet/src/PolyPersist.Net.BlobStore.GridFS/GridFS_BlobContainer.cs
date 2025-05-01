using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.GridFS
{
    internal class GridFS_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private GridFSBucket _gridFSBucket;
        private GridFS_BlobStore _database;
        public IMongoCollection<GridFSFileInfo> _filesCollection;
        public IMongoCollection<TBlob> _metadataCollection;

        public GridFS_BlobContainer(GridFSBucket gridFSBucket, GridFS_BlobStore database)
        {
            _gridFSBucket = gridFSBucket;
            _database = database;
            _filesCollection = _database._mongoDatabase.GetCollection<GridFSFileInfo>(gridFSBucket.Options.BucketName + ".files");
            _metadataCollection = _database._mongoDatabase.GetCollection<TBlob>(gridFSBucket.Options.BucketName + ".metadata");
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _gridFSBucket.Options.BucketName;

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            blob.etag = Guid.NewGuid().ToString();

            // insert metadata
            await _metadataCollection.InsertOneAsync(blob).ConfigureAwait(false);

            // Upload empty content to GridFS, to create a entity is database
            await _gridFSBucket.UploadFromStreamAsync(_makeId(blob), blob.fileName, content).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            GridFSFileInfo fileInfo = await _getFileInfo(blob.PartitionKey, blob.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not download, because it is does not exist");

            return await _gridFSBucket.OpenDownloadStreamAsync(fileInfo.Id);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            DeleteResult result = await _metadataCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new Exception($"Blob '{typeof(TBlob).Name}'{id} can not be removed because it is already removed or changed.");

            GridFSFileInfo fileInfo = await _getFileInfo(partitionKey, id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} cannot be deleted: it does not exist.");

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);
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
            GridFSFileInfo fileInfo = await _getFileInfo(blob.PartitionKey, blob.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not upload, because it is does not exist");

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);

            await _gridFSBucket.UploadFromStreamAsync(_makeId(blob), blob.fileName, content).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            await CollectionCommon.CheckBeforeUpdate(blob).ConfigureAwait(false);

            GridFSFileInfo fileInfo = await _getFileInfo(blob.PartitionKey, blob.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Entity '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            string oldETag = blob.etag;
            blob.etag = Guid.NewGuid().ToString();

            blob = await _metadataCollection.FindOneAndReplaceAsync(e => e.id == blob.id && e.PartitionKey == blob.PartitionKey && e.etag == oldETag, blob).ConfigureAwait(false);
            if (blob == null)
                throw new Exception($"Entity '{typeof(TBlob).Name}' {blob.id} can not be updated because it is already changed");
        }
    

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _gridFSBucket;
        }

        private ObjectId _makeId(TBlob blob)
            => _makeId(blob.PartitionKey, blob.id);

        private ObjectId _makeId(string partitionKey, string id)
            => new ObjectId($"{partitionKey}-{id}");

        private async Task<GridFSFileInfo> _getFileInfo(string partitionKey, string id)
        {
            IAsyncCursor<GridFSFileInfo> cursor = await _filesCollection.FindAsync(fi => fi.Id == _makeId(partitionKey, id)).ConfigureAwait(false);
            return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
        }
    }
}
