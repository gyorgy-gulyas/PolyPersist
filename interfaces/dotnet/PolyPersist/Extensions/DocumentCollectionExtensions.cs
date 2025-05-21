namespace PolyPersist.Net.Extensions
{
    public static class DocumentCollectionExtensions
    {
        /// <summary>
        /// Provides a strongly-typed IQueryable interface for the given IDocumentCollection.
        /// Throws InvalidCastException if the implementation does not return IQueryable.
        /// </summary>
        public static IQueryable<TDocument> AsQueryable<TDocument>(this IDocumentCollection<TDocument> collection)
            where TDocument : IDocument, new()
        {
            if (collection is null)
                throw new ArgumentNullException(nameof(collection));

            var query = collection.Query();

            if (query is IQueryable<TDocument> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidCastException($"The returned query object from table '{collection.Name}' is not an IQueryable<{typeof(TDocument).Name}>.");
        }
    }
}
