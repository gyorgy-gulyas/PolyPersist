namespace PolyPersist.Net.Common
{
    public static class CollectionCommon
    {

        public static void CheckBeforeInsert<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            if (entity is IValidable validable)
                Validator.Validate(validable);

            if (string.IsNullOrEmpty(entity.PartitionKey) == true)
                throw new Exception($"PartitionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.etag) == false)
                throw new Exception($"ETag is already filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");
        }

        public static void CheckBeforeUpdate<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            if (entity is IValidable validable)
                Validator.Validate(validable);

            if (string.IsNullOrEmpty(entity.PartitionKey) == true)
                throw new Exception($"PartitionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.etag) == true)
                throw new Exception($"ETag must be filled at Update operation in entity '{typeof(TEntity).Name}' id: {entity.id}");
        }

        /// <summary>
        /// Optimistic-concurrency guard: the currently stored entity must still exist and carry
        /// the same etag the caller last read. One place for the "does not exist" / "already
        /// changed" semantics so every store behaves identically.
        /// </summary>
        public static void CheckEtagMatch<TEntity>(TEntity? stored, TEntity incoming)
            where TEntity : IEntity
        {
            if (stored is null)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {incoming.id} can not be updated because it does not exist");

            CheckEtagMatch(stored.etag, incoming);
        }

        /// <summary>Overload for stores that already hold the stored etag (not the whole entity).</summary>
        public static void CheckEtagMatch<TEntity>(string storedEtag, TEntity incoming)
            where TEntity : IEntity
        {
            if (storedEtag != incoming.etag)
                throw new Exception($"Entity '{typeof(TEntity).Name}' {incoming.id} can not be updated because it is already changed");
        }
    }
}
