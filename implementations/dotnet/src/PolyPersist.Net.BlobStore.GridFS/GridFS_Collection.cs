using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.GridFS
{
    internal class GridFS_Collection<TEntity> : IBlobCollection<TEntity>
        where TEntity : IEntity, new()
    {
        private GridFSBucket _gridFSBucket;
        private GridFS_Database _database;
        public IMongoCollection<GridFSFileInfo> _filesCollection;
        public IMongoCollection<TEntity> _metadataCollection;

        public GridFS_Collection(GridFSBucket gridFSBucket, GridFS_Database database)
        {
            _gridFSBucket = gridFSBucket;
            _database = database;
            _filesCollection = _database._mongoDatabase.GetCollection<GridFSFileInfo>(gridFSBucket.Options.BucketName + ".files");
            _metadataCollection = _database._mongoDatabase.GetCollection<TEntity>(gridFSBucket.Options.BucketName + ".metadata");
        }

        /// <inheritdoc/>
        string ICollection<TEntity>.Name => _gridFSBucket.Options.BucketName;

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Insert(TEntity entity)
        {
            await CollectionCommon.CheckBeforeInsert(entity).ConfigureAwait(false);

            entity.etag = Guid.NewGuid().ToString();

            // insertt metadata
            await _metadataCollection.InsertOneAsync(entity).ConfigureAwait(false);

            // Upload empty content to GridFS, to create a entity is database
            IBlob blob = (IBlob)entity;
            await _gridFSBucket.UploadFromBytesAsync(_makeId(entity), blob.fileName, [0]).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            GridFSFileInfo fileInfo = await _getFileInfo(entity.PartitionKey, entity.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it does not exist.");

            string oldETag = entity.etag;
            entity.etag = Guid.NewGuid().ToString();

            entity = await _metadataCollection.FindOneAndReplaceAsync(e => e.id == entity.id && e.PartitionKey == entity.PartitionKey && e.etag != oldETag, entity).ConfigureAwait(false);
            if (entity == null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed");
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(TEntity entity)
        {
            return (this as ICollection<TEntity>).Delete(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Delete(string id, string partitionKey)
        {
            DeleteResult result = await _metadataCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new Exception($"Entity '{typeof(TEntity).Name}'{id} can not be removed because it is already removed or changed.");

            GridFSFileInfo fileInfo = await _getFileInfo(partitionKey, id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be delete because it does not exist.");

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<TEntity> ICollection<TEntity>.Find(string id, string partitionKey)
        {
            IAsyncCursor<TEntity> cursor = await _metadataCollection.FindAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            TEntity entity = await cursor.FirstOrDefaultAsync().ConfigureAwait(false);

            return entity;
        }

        /// <inheritdoc/>
        TQuery ICollection<TEntity>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TEntity>).IsAssignableFrom(typeof(TQuery));
            if (isQueryable == false)
                throw new Exception($"TQuery is must be 'IQueryable<TEntity>' in dotnet implementation");

            return (TQuery)_metadataCollection.AsQueryable();
        }

        /// <inheritdoc/>
        object ICollection<TEntity>.GetUnderlyingImplementation()
        {
            return _gridFSBucket;
        }

        async Task IBlobCollection<TEntity>.UploadContent(TEntity entity, Stream source)
        {
            GridFSFileInfo fileInfo = await _getFileInfo(entity.PartitionKey, entity.id).ConfigureAwait(false);
            if (fileInfo == null )
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not upload, because it is does not exist");

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);

            IBlob blob = (IBlob)entity;
            await _gridFSBucket.UploadFromStreamAsync(_makeId(entity), blob.fileName, source).ConfigureAwait(false);
        }

        async Task IBlobCollection<TEntity>.DownloadContentTo(TEntity entity, Stream destination)
        {
            GridFSFileInfo fileInfo = await _getFileInfo(entity.PartitionKey, entity.id).ConfigureAwait(false);
            if (fileInfo == null )
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not download, because it is does not exist");

            await _gridFSBucket.DownloadToStreamAsync(fileInfo.Id, destination).ConfigureAwait(false);
        }

        private ObjectId _makeId(TEntity blob)
            => _makeId(blob.PartitionKey, blob.id);

        private ObjectId _makeId(string partitionKey, string id)
            => new ObjectId($"{partitionKey}/{id}");

        private async Task<GridFSFileInfo> _getFileInfo(string partitionKey, string id)
        {
            IAsyncCursor<GridFSFileInfo> cursor = await _filesCollection.FindAsync(fi => fi.Id == _makeId(partitionKey, id)).ConfigureAwait(false);
            return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
        }
    }
}
