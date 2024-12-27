using MongoDB.Driver;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.DocumentStore.MongoDB
{
    internal class MongoDB_Collection<TEntity> : IEntityCollection<TEntity>
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
        string ICollection.Name => _mongoCollection.CollectionNamespace.CollectionName;

        /// <inheritdoc/>
        async Task IEntityCollection<TEntity>.Insert(TEntity entity)
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
        async Task IEntityCollection<TEntity>.Update(TEntity entity)
        {
            if (entity is IValidable validable)
                await Validator.Validate(validable).ConfigureAwait(false);

            if (string.IsNullOrEmpty(entity.etag) == true)
                throw new Exception($"ETag must be filled at Update operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            string oldETag = entity.etag;
            entity.etag = Guid.NewGuid().ToString();

            entity = await _mongoCollection.FindOneAndReplaceAsync(e => e.id == entity.id && e.PartitionKey == entity.PartitionKey && e.etag != entity.etag, entity).ConfigureAwait(false);
            if(entity== null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed or removed.");
        }

        /// <inheritdoc/>
        Task IEntityCollection<TEntity>.Delete(TEntity entity)
        {
            return (this as IEntityCollection<TEntity>).Delete(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        async Task IEntityCollection<TEntity>.Delete(string id, string partitionKey)
        {
            DeleteResult result = await _mongoCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new Exception($"Entity '{typeof(TEntity).Name}'{id} can not be removed because it is already removed or changed.");
        }

        /// <inheritdoc/>
        async Task<TEntity> IEntityCollection<TEntity>.Find(string id, string partitionKey)
        {
            IAsyncCursor<TEntity> cursor = await _mongoCollection.FindAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            TEntity entity = await cursor.FirstOrDefaultAsync().ConfigureAwait(false);

            return entity;
        }
    }
}
