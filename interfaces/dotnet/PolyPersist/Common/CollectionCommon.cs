﻿namespace PolyPersist.Net.Common
{
    public static class CollectionCommon
    {

        public static async Task CheckBeforeInsert<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            if (entity is IValidable validable)
                await Validator.Validate(validable).ConfigureAwait(false);

            if (string.IsNullOrEmpty(entity.PartitionKey) == true)
                throw new Exception($"PartitionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.etag) == false)
                throw new Exception($"ETag is already filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");
        }

        public static async Task CheckBeforeUpdate<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            if (entity is IValidable validable)
                await Validator.Validate(validable).ConfigureAwait(false);

            if (string.IsNullOrEmpty(entity.PartitionKey) == true)
                throw new Exception($"PartitionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.etag) == true)
                throw new Exception($"ETag must be filled at Update operation in entity '{typeof(TEntity).Name}' id: {entity.id}");
        }
    }
}
