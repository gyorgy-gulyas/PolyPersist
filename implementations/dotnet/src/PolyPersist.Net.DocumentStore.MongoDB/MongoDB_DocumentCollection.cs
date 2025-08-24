using MongoDB.Driver;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.DocumentStore.MongoDB
{
    internal class MongoDB_DocumentCollection<TDocument> : IDocumentCollection<TDocument>
        where TDocument : IDocument, new()
    {
        private readonly IMongoCollection<TDocument> _mongoCollection;
        private readonly MongoDB_DocumentStore _store;

        public MongoDB_DocumentCollection(IMongoCollection<TDocument> mongoCollection, MongoDB_DocumentStore store)
        {
            _mongoCollection = mongoCollection;
            _store = store;
        }

        /// <inheritdoc/>
        string IDocumentCollection<TDocument>.Name => _mongoCollection.CollectionNamespace.CollectionName;
        /// <inheritdoc/>
        IStore IDocumentCollection<TDocument>.ParentStore => _store;

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Insert(TDocument document)
        {
            CollectionCommon.CheckBeforeInsert(document);
            document.etag = Guid.NewGuid().ToString();
            document.LastUpdate = DateTime.UtcNow;

            try
            {
                await _mongoCollection.InsertOneAsync(document).ConfigureAwait(false);
            }
            catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
            {
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} cannot be inserted, beacuse of duplicate key");
            }
            catch
            {
                throw;
            }
        }

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Update(TDocument document)
        {
            CollectionCommon.CheckBeforeUpdate(document);

            string oldETag = document.etag;
            document.etag = Guid.NewGuid().ToString();
            document.LastUpdate = DateTime.UtcNow;

            var result = await _mongoCollection.ReplaceOneAsync(e => e.id == document.id && e.PartitionKey == document.PartitionKey && e.etag == oldETag, document).ConfigureAwait(false);
            if (result.IsAcknowledged == false || result.ModifiedCount != 1)
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} can not be updated because does not exist.");
        }

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Delete(string partitionKey, string id)
        {
            DeleteResult result = await _mongoCollection.DeleteOneAsync(e => e.id == id && e.PartitionKey == partitionKey).ConfigureAwait(false);
            if (result.IsAcknowledged == false)
                throw new Exception($"Document '{typeof(TDocument).Name}' {id} can not be removed. Database refused to acknowledge the operation.");

            if (result.DeletedCount != 1)
                throw new Exception($"Document'{typeof(TDocument).Name}'{id} can not be removed because it is already removed or changed.");
        }

        /// <inheritdoc/>
        async Task<TDocument> IDocumentCollection<TDocument>.Find(string partitionKey, string id)
        {
            TDocument document = await _mongoCollection
                .Find(e => e.id == id && e.PartitionKey == partitionKey)
                .FirstOrDefaultAsync()
                .ConfigureAwait(false);

            return document;
        }

        /// <inheritdoc/>
        object IDocumentCollection<TDocument>.Query<T>()
        {
            return _mongoCollection.OfType<T>().AsQueryable();
        }

        /// <inheritdoc/>
        object IDocumentCollection<TDocument>.GetUnderlyingImplementation()
        {
            return _mongoCollection;
        }
    }
}
