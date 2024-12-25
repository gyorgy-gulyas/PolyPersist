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
        async Task<IResult> ICollection<TEntity>.InsertOne(TEntity entity)
        {
            if (entity is IValidabale validable)
            {
                Result result = await Validator.Validate(validable).ConfigureAwait(false);
                if( result.Succeeded == false )
                    return result;
            }

            if (string.IsNullOrEmpty(entity.etag) == false)
                return Result.Error("ETag is already filled.");

            entity.etag = Guid.NewGuid().ToString();

            if ( string.IsNullOrEmpty( entity.id) == true)
                entity.id = Guid.NewGuid().ToString();

            await _mongoCollection.InsertOneAsync(entity).ConfigureAwait(false);
            return Result.Ok();
        }

        /// <inheritdoc/>
        async Task<IResult> ICollection<TEntity>.UpdateOne(TEntity entity)
        {
            if (entity is IValidabale validable)
            {
                Result result = await Validator.Validate(validable).ConfigureAwait(false);
                if (result.Succeeded == false)
                    return result;
            }
            string oldETag = entity.etag;
            if (await _mongoCollection.CountDocumentsAsync(e => e.id == entity.id && e.PartitionKey == entity.PartitionKey && e.etag != entity.etag).ConfigureAwait(false) > 0)
                return Result.Error($"Entity {entity.id} can not be updated because it is already changed or removed.");

            entity.etag = Guid.NewGuid().ToString();
            var replace = await _mongoCollection.ReplaceOneAsync(e => e.id == e.id && e.PartitionKey == entity.PartitionKey && e.etag == oldETag, entity).ConfigureAwait(false);

            if (replace.IsAcknowledged == false)
                return Result.Error($"Entity {entity.id} can not be updated. Database refused to acknowledge the operation.");

            if (replace.ModifiedCount != 1)
                return Result.Error($"Entity {entity.id} can not be updated because it is already changed or removed.");

            return Result.Ok();
        }

        /// <inheritdoc/>
        Task<IResult> ICollection<TEntity>.DeleteOne(TEntity entity)
        {
            return (this as ICollection<TEntity>).DeleteOne(entity.id,entity.PartitionKey);
        }

        /// <inheritdoc/>
        async Task<IResult> ICollection<TEntity>.DeleteOne(string id, string partitionKey)
        {
            DeleteResult result = await _mongoCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                return Result.Error($"Document {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                return Result.Error($"Document {id} can not be removed because it is already removed or changed.");

            return Result.Ok();
        }

        /// <inheritdoc/>
        async Task<TEntity> ICollection<TEntity>.FindOne(string id, string partitionKey)
        {
            IAsyncCursor<TEntity> documentCursor = await _mongoCollection.FindAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            TEntity entity = await documentCursor.FirstOrDefaultAsync().ConfigureAwait(false);

            return entity;
        }
    }
}
