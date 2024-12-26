using MongoDB.Driver;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.MongoDB
{
    internal class MongoDB_Collection<TEntity> : ICollection<TEntity>
        where TEntity : IEntity
    {
        private readonly IMongoCollection<TEntity> _mongoCollection;
        private readonly MongoDB_Database _mongoDB_Database;

        public MongoDB_Collection(IMongoCollection<TEntity> mongoCollection, MongoDB_Database mongoDB_Database)
        {
            _mongoCollection = mongoCollection;
            _mongoDB_Database = mongoDB_Database;
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.InsertOne(TEntity entity)
        {
            if (entity is IValidable validable)
                await Validator.Validate(validable).ConfigureAwait(false);

            if (string.IsNullOrEmpty(entity.etag) == false)
                throw new Exception($"ETag is already filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.PartitionKey) == true)
                throw new Exception($"PartionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            entity.etag = Guid.NewGuid().ToString();

            if (string.IsNullOrEmpty(entity.id) == true)
                entity.id = Guid.NewGuid().ToString();

            await _mongoCollection.InsertOneAsync(entity).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.UpdateOne(TEntity entity)
        {
            if (entity is IValidable validable)
                await Validator.Validate(validable).ConfigureAwait(false);

            string oldETag = entity.etag;
            entity.etag = Guid.NewGuid().ToString();

            entity = await _mongoCollection.FindOneAndReplaceAsync(e => e.id == entity.id && e.PartitionKey == entity.PartitionKey && e.etag != entity.etag, entity).ConfigureAwait(false);
            if(entity== null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed or removed.");
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.DeleteOne(TEntity entity)
        {
            return (this as ICollection<TEntity>).DeleteOne(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.DeleteOne(string id, string partitionKey)
        {
            DeleteResult result = await _mongoCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new Exception($"Entity '{typeof(TEntity).Name}'{id} can not be removed because it is already removed or changed.");
        }

        /// <inheritdoc/>
        async Task<TEntity> ICollection<TEntity>.FindOne(string id, string partitionKey)
        {
            IAsyncCursor<TEntity> cursor = await _mongoCollection.FindAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            TEntity entity = await cursor.FirstOrDefaultAsync().ConfigureAwait(false);

            return entity;
        }
    }
}
