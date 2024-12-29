using MongoDB.Driver;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.DocumentStore.MongoDB
{
    internal class MongoDB_Collection<TEntity> : ICollection<TEntity>
        where TEntity : IEntity, new()
    {
        private readonly IMongoCollection<TEntity> _mongoCollection;
        private readonly MongoDB_Database _mongoDB_Database;

        public MongoDB_Collection(IMongoCollection<TEntity> mongoCollection, MongoDB_Database mongoDB_Database)
        {
            _mongoCollection = mongoCollection;
            _mongoDB_Database = mongoDB_Database;
        }

        /// <inheritdoc/>
        string ICollection<TEntity>.Name => _mongoCollection.CollectionNamespace.CollectionName;

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Insert(TEntity entity)
        {
            await CollectionCommon.CheckBeforeInsert(entity).ConfigureAwait(false);
            entity.etag = Guid.NewGuid().ToString();

            await _mongoCollection.InsertOneAsync(entity).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Update(TEntity entity)
        {
            await CollectionCommon.CheckBeforeUpdate(entity).ConfigureAwait(false);

            string oldETag = entity.etag;
            entity.etag = Guid.NewGuid().ToString();

            entity = await _mongoCollection.FindOneAndReplaceAsync(e => e.id == entity.id && e.PartitionKey == entity.PartitionKey && e.etag != entity.etag, entity).ConfigureAwait(false);
            if(entity== null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {entity.id} can not be updated because it is already changed or removed.");
        }

        /// <inheritdoc/>
        Task ICollection<TEntity>.Delete(TEntity entity)
        {
            return (this as ICollection<TEntity>).Delete(entity.id, entity.PartitionKey);
        }

        /// <inheritdoc/>
        async Task ICollection<TEntity>.Delete(string id, string partitionKey)
        {
            DeleteResult result = await _mongoCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new Exception($"Entity '{typeof(TEntity).Name}'{id} can not be removed because it is already removed or changed.");
        }

        /// <inheritdoc/>
        async Task<TEntity> ICollection<TEntity>.Find(string id, string partitionKey)
        {
            IAsyncCursor<TEntity> cursor = await _mongoCollection.FindAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            TEntity entity = await cursor.FirstOrDefaultAsync().ConfigureAwait(false);

            return entity;
        }

        /// <inheritdoc/>
        TQuery ICollection<TEntity>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TEntity>).IsAssignableFrom(typeof(TQuery));
            if(isQueryable==false)
                throw new Exception($"TQuery is must be 'IQueryable<TEntity>' in dotnet implementation");

            return (TQuery)_mongoCollection.AsQueryable();
        }

        /// <inheritdoc/>
        object ICollection<TEntity>.GetUnderlyingImplementation()
        {
            return _mongoCollection;
        }
    }
}
