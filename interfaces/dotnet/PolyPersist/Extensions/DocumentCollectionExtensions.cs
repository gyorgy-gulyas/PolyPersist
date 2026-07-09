namespace PolyPersist.Net.Extensions
{
    public static class DocumentCollectionExtensions
    {
        /// <summary>
        /// Strongly-typed CROSS-PARTITION queryable for the collection (spans every partition).
        /// Throws InvalidCastException if the implementation does not return IQueryable.
        /// </summary>
        public static IQueryable<TDocument> AsQueryable<TDocument>(this IDocumentCollection<TDocument> collection)
            where TDocument : IDocument, new()
        {
            if (collection is null)
                throw new ArgumentNullException(nameof(collection));

            var query = collection.QueryCrossPartition();

            if (query is IQueryable<TDocument> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidCastException($"The returned query object from table '{collection.Name}' is not an IQueryable<{typeof(TDocument).Name}>.");
        }

        /// <summary>
        /// Strongly-typed queryable SCOPED to a single partition (already filtered by partitionKey).
        /// Throws InvalidCastException if the implementation does not return IQueryable.
        /// </summary>
        public static IQueryable<TDocument> AsQueryable<TDocument>(this IDocumentCollection<TDocument> collection, string partitionKey)
            where TDocument : IDocument, new()
        {
            if (collection is null)
                throw new ArgumentNullException(nameof(collection));

            var query = collection.Query(partitionKey);

            if (query is IQueryable<TDocument> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidCastException($"The returned query object from table '{collection.Name}' is not an IQueryable<{typeof(TDocument).Name}>.");
        }

        /// <summary>
        /// Strongly-typed CROSS-PARTITION queryable narrowed to a document subtype (spans every partition).
        /// Throws InvalidCastException if the implementation does not return IQueryable.
        /// </summary>
        public static IQueryable<TQueryType> AsQueryable<TQueryType, TDocument>(this IDocumentCollection<TDocument> collection)
            where TDocument : IDocument, new()
            where TQueryType : TDocument, new()
        {
            if (collection is null)
                throw new ArgumentNullException(nameof(collection));

            var query = collection.QueryCrossPartition().OfType<TQueryType>();

            if (query is IQueryable<TQueryType> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidCastException($"The returned query object from table '{collection.Name}' is not an IQueryable<{typeof(TDocument).Name}>.");
        }
    }
}
