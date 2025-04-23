using PolyPersist.Net.Common;
using System.Text.Json;

namespace PolyPersist.Net.DocumentStore.Memory
{
    internal class Memory_DocumentCollection<TDocument> : IDocumentCollection<TDocument>
        where TDocument : IDocument, new()
    {
        internal string _name;
        internal _CollectionData _collectionData;
        internal Memory_DocumentStore _dataStore;

        internal Memory_DocumentCollection(string name, _CollectionData collectionData, Memory_DocumentStore dataStore)
        {
            _name = name;
            _collectionData = collectionData;
            _dataStore = dataStore;
        }

        /// <inheritdoc/>
        string IDocumentCollection<TDocument>.Name => _name;

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Insert(TDocument document)
        {
            await CollectionCommon.CheckBeforeInsert(document).ConfigureAwait(false);

            document.etag = Guid.NewGuid().ToString();

            if (string.IsNullOrEmpty(document.id) == true)
                document.id = Guid.NewGuid().ToString();

            _RowData row = new()
            {
                id = document.id,
                partionKey = document.PartitionKey,
                etag = document.etag,
                Value = JsonSerializer.Serialize(document, typeof(TDocument), JsonOptionsProvider.Options)
            };

            _collectionData.MapOfDocments.Add((document.id, document.PartitionKey), row);
            _collectionData.ListOfDocments.Add(row);
        }

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Update(TDocument document)
        {
            await CollectionCommon.CheckBeforeUpdate(document).ConfigureAwait(false);

            if (_collectionData.MapOfDocments.TryGetValue((document.id, document.PartitionKey), out _RowData row) == false)
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} can not be updated because it is already removed");

            if (row.etag != document.etag)
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} can not be updated because it is already changed");

            document.etag = Guid.NewGuid().ToString();
            row.etag = document.etag;
            row.Value = JsonSerializer.Serialize(document, typeof(TDocument), JsonOptionsProvider.Options);
        }

        /// <inheritdoc/>
        Task IDocumentCollection<TDocument>.Delete(string id, string partitionKey)
        {
            if (_collectionData.MapOfDocments.TryGetValue((id, partitionKey), out _RowData row) == false)
                throw new Exception($"Document '{typeof(TDocument).Name}' {id} can not be removed because it is already removed");

            _collectionData.MapOfDocments.Remove((id, partitionKey));
            _collectionData.ListOfDocments.Remove(row);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TDocument> IDocumentCollection<TDocument>.Find(string id, string partitionKey)
        {
            if (_collectionData.MapOfDocments.TryGetValue((id, partitionKey), out _RowData row) == true)
                return Task.FromResult(JsonSerializer.Deserialize<TDocument>(row.Value, JsonOptionsProvider.Options));

            return Task.FromResult(default(TDocument));
        }

        /// <inheritdoc/>
        TQuery IDocumentCollection<TDocument>.Query<TQuery>()
        {
            bool isQueryable = typeof(IQueryable<TDocument>).IsAssignableFrom(typeof(TQuery));
            if (isQueryable == false)
                throw new Exception($"TQuery is must be 'IQueryable<TDocument>' in dotnet implementation");

            return (TQuery)_collectionData.ListOfDocments.AsQueryable();
        }

        /// <inheritdoc/>
        object IDocumentCollection<TDocument>.GetUnderlyingImplementation()
        {
            return _collectionData;
        }
    }
}
