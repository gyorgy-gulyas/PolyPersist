using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.BlobStore.GridFS
{
    internal class GridFS_Collection<TEntity> : ICollection<TEntity>
        where TEntity : IEntity, new()
    {
        private GridFSBucket _gridFSBucket;
        private GridFS_Database _database;
        public IMongoCollection<GridFSFileInfo> _filesCollection;

        public GridFS_Collection(GridFSBucket gridFSBucket, GridFS_Database database)
        {
            _gridFSBucket = gridFSBucket;
            _database = database;
            _filesCollection = _database._mongoDatabase.GetCollection<GridFSFileInfo>(gridFSBucket.Options.BucketName + ".files");
        }

        /// <inheritdoc/>
        string ICollection<TEntity>.Name => _gridFSBucket.Options.BucketName;

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Insert(TEntity entity)
        {
            await CollectionCommon.CheckBeforeInsert(entity).ConfigureAwait(false);

            IFile file = (IFile)entity;
            file.etag = Guid.NewGuid().ToString();

            // Upload the file to GridFS
            await _gridFSBucket.UploadFromStreamAsync(id: _makeId(file), filename: file.fileName, file.content, new GridFSUploadOptions() { Metadata = file.ToBsonDocument() }).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            IFile file = (IFile)entity;
            GridFSFileInfo fileInfo = await _getFileInfo(file.PartitionKey, file.id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it does not exist.");

            TEntity oldEntity = BsonSerializer.Deserialize<TEntity>(fileInfo.Metadata, null);
            if (oldEntity.etag != entity.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed.");

            file.etag = Guid.NewGuid().ToString();

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);
            await _gridFSBucket.UploadFromStreamAsync(id: _makeId(file), filename: file.fileName, file.content, new GridFSUploadOptions() { Metadata = file.ToBsonDocument() }).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(TEntity entity)
        {
            return (this as ICollection<TEntity>).Delete(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Delete(string id, string partitionKey)
        {
            GridFSFileInfo fileInfo = await _getFileInfo( partitionKey, id).ConfigureAwait(false);
            if (fileInfo == null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be delete because it does not exist.");

            await _gridFSBucket.DeleteAsync(fileInfo.Id).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<TEntity> ICollection<TEntity>.Find(string id, string partitionKey)
        {
            GridFSFileInfo fileInfo = await _getFileInfo(partitionKey, id).ConfigureAwait(false);
            if (fileInfo == null)
                throw default;

            TEntity entity = BsonSerializer.Deserialize<TEntity>(fileInfo.Metadata, null);

            IFile file = (IFile)entity;
            file.content = new MemoryStream();
            await _gridFSBucket.DownloadToStreamAsync(fileInfo.Id, file.content).ConfigureAwait(false);

            return entity;
        }

        private ObjectId _makeId(IFile file)
            => _makeId(file.PartitionKey, file.id);

        private ObjectId _makeId(string partitionKey, string id)
            => new ObjectId($"{partitionKey}/{id}");

        private async Task<GridFSFileInfo> _getFileInfo(string partitionKey, string id)
        {
            IAsyncCursor<GridFSFileInfo> cursor = await _filesCollection.FindAsync(fi => fi.Id == _makeId(partitionKey, id)).ConfigureAwait(false);
            return await cursor.FirstOrDefaultAsync().ConfigureAwait(false);
        }

    }
}
