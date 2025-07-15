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
        IStore IDocumentCollection<TDocument>.ParentStore => _dataStore;

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Insert(TDocument document)
        {
            await CollectionCommon.CheckBeforeInsert(document).ConfigureAwait(false);

            document.etag = Guid.NewGuid().ToString();
            document.LastUpdate = DateTime.UtcNow;

            if (string.IsNullOrEmpty(document.id) == true)
                document.id = Guid.NewGuid().ToString();
            else if (_collectionData.MapOfDocments.ContainsKey(document.id) == true)
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} cannot be inserted, beacuse of duplicate key");

            _RowData row = new()
            {
                id = document.id,
                partitionKey = document.PartitionKey,
                etag = document.etag,
                Value = JsonSerializer.Serialize(document, typeof(TDocument), JsonOptionsProvider.Options)
            };

            _collectionData.MapOfDocments.Add(document.id, row);
            _collectionData.ListOfDocments.Add(row);
        }

        /// <inheritdoc/>
        async Task IDocumentCollection<TDocument>.Update(TDocument document)
        {
            await CollectionCommon.CheckBeforeUpdate(document).ConfigureAwait(false);

            if (_collectionData.MapOfDocments.TryGetValue(document.id, out _RowData row) == false)
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} can not be updated because does not exist");

            if (row.etag != document.etag)
                throw new Exception($"Document '{typeof(TDocument).Name}' {document.id} can not be updated because it is already changed");

            document.etag = Guid.NewGuid().ToString();
            document.LastUpdate = DateTime.Now;

            row.etag = document.etag;
            row.Value = JsonSerializer.Serialize(document, typeof(TDocument), JsonOptionsProvider.Options);
        }

        /// <inheritdoc/>
        Task IDocumentCollection<TDocument>.Delete(string partitionKey, string id)
        {
            if (_collectionData.MapOfDocments.TryGetValue(id, out _RowData row) == false)
                throw new Exception($"Document '{typeof(TDocument).Name}' {id} can not be removed because it is already removed");

            _collectionData.MapOfDocments.Remove(id);
            _collectionData.ListOfDocments.Remove(row);

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<TDocument> IDocumentCollection<TDocument>.Find(string partitionKey, string id)
        {
            if (_collectionData.MapOfDocments.TryGetValue(id, out _RowData row) == true)
                return Task.FromResult(JsonSerializer.Deserialize<TDocument>(row.Value, JsonOptionsProvider.Options));

            return Task.FromResult(default(TDocument));
        }

        /// <inheritdoc/>
        object IDocumentCollection<TDocument>.Query()
        {
            return _collectionData
                .ListOfDocments
                .Select(data => JsonSerializer.Deserialize<TDocument>(data.Value, JsonOptionsProvider.Options))
                .AsQueryable();
        }

        /// <inheritdoc/>
        object IDocumentCollection<TDocument>.GetUnderlyingImplementation()
        {
            return _collectionData;
        }
    }
}
