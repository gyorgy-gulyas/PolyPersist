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
                throw new InvalidRequestException($"PartitionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.etag) == false)
                throw new InvalidRequestException($"ETag is already filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");
        }

        public static void CheckBeforeUpdate<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            if (entity is IValidable validable)
                Validator.Validate(validable);

            if (string.IsNullOrEmpty(entity.PartitionKey) == true)
                throw new InvalidRequestException($"PartitionKey must be filled at Insert operation in entity '{typeof(TEntity).Name}' id: {entity.id}");

            if (string.IsNullOrEmpty(entity.etag) == true)
                throw new InvalidRequestException($"ETag must be filled at Update operation in entity '{typeof(TEntity).Name}' id: {entity.id}");
        }

        /// <summary>
        /// Assigns a client-generated id when the caller did not supply one.
        /// A deferred transaction calls this when the operation is queued, so the caller holds a
        /// usable id long before Commit() writes the entity; the store then finds it already set.
        /// </summary>
        public static void AssignIdIfMissing<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            if (string.IsNullOrEmpty(entity.id) == true)
                entity.id = Guid.NewGuid().ToString();
        }

        /// <summary>
        /// Stamps an entity that is about to be written for the first time: id (if missing), a fresh
        /// etag and LastUpdate. The etag is deliberately NOT assigned any earlier than the write,
        /// because <see cref="CheckBeforeInsert"/> requires it to be empty until then.
        /// </summary>
        public static void StampForInsert<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            AssignIdIfMissing(entity);
            entity.etag = Guid.NewGuid().ToString();
            entity.LastUpdate = DateTime.UtcNow;
        }

        /// <summary>Stamps a fresh etag and LastUpdate on an entity that is about to be overwritten.</summary>
        public static void StampForUpdate<TEntity>(TEntity entity)
            where TEntity : IEntity
        {
            entity.etag = Guid.NewGuid().ToString();
            entity.LastUpdate = DateTime.UtcNow;
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
                throw new NotFoundException($"Entity '{typeof(TEntity).Name}' {incoming.id} can not be updated because it does not exist");

            CheckEtagMatch(stored.etag, incoming);
        }

        /// <summary>Overload for stores that already hold the stored etag (not the whole entity).</summary>
        public static void CheckEtagMatch<TEntity>(string storedEtag, TEntity incoming)
            where TEntity : IEntity
        {
            if (storedEtag != incoming.etag)
                throw new ConcurrencyConflictException($"Entity '{typeof(TEntity).Name}' {incoming.id} can not be updated because it is already changed");
        }
    }
}
