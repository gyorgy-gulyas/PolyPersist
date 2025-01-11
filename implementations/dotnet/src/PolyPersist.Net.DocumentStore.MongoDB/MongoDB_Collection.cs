using MongoDB.Driver;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.DocumentStore.MongoDB
{
    internal class MongoDB_Collection<TDocument> : IDocumentCollection<TDocument>
        where TDocument : IDocument, new()
    {
        private readonly IMongoCollection<TDocument> _mongoCollection;
        private readonly MongoDB_Database _mongoDB_Database;

        public MongoDB_Collection(IMongoCollection<TDocument> mongoCollection, MongoDB_Database mongoDB_Database)
        {
            _mongoCollection = mongoCollection;
            _mongoDB_Database = mongoDB_Database;
        }

        /// <inheritdoc/>
        string IDocumentCollection<TDocument>.Name => _mongoCollection.CollectionNamespace.CollectionName;

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Insert(TDocument document)
        {
            await CollectionCommon.CheckBeforeInsert(document).ConfigureAwait(false);
            document.etag = Guid.NewGuid().ToString();

            await _mongoCollection.InsertOneAsync(document).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Update(TDocument document)
        {
            await CollectionCommon.CheckBeforeUpdate(document).ConfigureAwait(false);

            string oldETag = document.etag;
            document.etag = Guid.NewGuid().ToString();

            document = await _mongoCollection.FindOneAndReplaceAsync(e => e.id == document.id && e.PartitionKey == document.PartitionKey && e.etag != oldETag, document).ConfigureAwait(false);
            if(document== null)
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} can not be updated because it is already changed or removed.");
        }

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Delete(string id, string partitionKey)
        {
            DeleteResult result = await _mongoCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new Exception($"Document '{typeof(TDocument).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new Exception($"Document'{typeof(TDocument).Name}'{id} can not be removed because it is already removed or changed.");
        }

        /// <inheritdoc/>
        async Task<TDocument> IDocumentCollection<TDocument>.Find(string id, string partitionKey)
        {
            IAsyncCursor<TDocument> cursor = await _mongoCollection.FindAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            TDocument document = await cursor.FirstOrDefaultAsync().ConfigureAwait(false);

            return document;
        }

        /// <inheritdoc/>
        TQuery IDocumentCollection<TDocument>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TDocument>).IsAssignableFrom(typeof(TQuery));
            if(isQueryable==false)
                throw new Exception($"TQuery is must be 'IQueryable<TDocument>' in dotnet implementation");

            return (TQuery)_mongoCollection.AsQueryable();
        }

        /// <inheritdoc/>
        object IDocumentCollection<TDocument>.GetUnderlyingImplementation()
        {
            return _mongoCollection;
        }
    }
}
