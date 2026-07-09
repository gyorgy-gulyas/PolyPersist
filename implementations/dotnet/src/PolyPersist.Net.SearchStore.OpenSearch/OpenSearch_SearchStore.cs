using OpenSearch.Client;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.SearchStore.OpenSearch
{
    /// <summary>
    /// Full-text search store on OpenSearch (the native, production-scale backend). Only the thin,
    /// portable surface (index / delete / simple full-text search with paging) is exposed here;
    /// engine-specific power (relevance tuning, facets/aggregations, highlighting, suggesters) is
    /// reached through GetUnderlyingImplementation with the OpenSearch client.
    /// </summary>
    public class OpenSearch_SearchStore : ISearchStore
    {
        internal readonly OpenSearchClient _client;

        /// <param name="connectionString">the OpenSearch base URL, e.g. http://localhost:9200 .</param>
        public OpenSearch_SearchStore(string connectionString)
            : this(new OpenSearchClient(new ConnectionSettings(new Uri(connectionString))))
        {
        }

        /// <summary>Uses a pre-configured client (custom auth, pooling, TLS).</summary>
        public OpenSearch_SearchStore(OpenSearchClient client)
        {
            _client = client;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Search;
        /// <inheritdoc/>
        string IStore.ProviderName => "Search(OpenSearch)";

        /// <inheritdoc/>
        async Task<bool> ISearchStore.IsIndexExists(string indexName)
        {
            var response = await _client.Indices.ExistsAsync(indexName).ConfigureAwait(false);
            return response.Exists;
        }

        /// <inheritdoc/>
        async Task<ISearchIndex<TDocument>> ISearchStore.GetIndexByName<TDocument>(string indexName)
        {
            if (await ((ISearchStore)this).IsIndexExists(indexName).ConfigureAwait(false) == false)
                throw new NotFoundException($"Index '{indexName}' does not exist in the OpenSearch store");

            return _NewIndex<TDocument>(indexName);
        }

        /// <inheritdoc/>
        async Task<ISearchIndex<TDocument>> ISearchStore.CreateIndex<TDocument>(string indexName)
        {
            if (await ((ISearchStore)this).IsIndexExists(indexName).ConfigureAwait(false) == true)
                throw new DuplicateKeyException($"Index '{indexName}' already exists in the OpenSearch store");

            // Dynamic mapping is enough for the portable surface (text fields become searchable text).
            var response = await _client.Indices.CreateAsync(indexName).ConfigureAwait(false);
            if (response.IsValid == false)
                throw new PolyPersistException($"Failed to create index '{indexName}': {response.ServerError?.Error?.Reason ?? response.DebugInformation}");

            return _NewIndex<TDocument>(indexName);
        }

        /// <inheritdoc/>
        async Task ISearchStore.DropIndex(string indexName)
        {
            if (await ((ISearchStore)this).IsIndexExists(indexName).ConfigureAwait(false) == false)
                throw new NotFoundException($"Index '{indexName}' does not exist in the OpenSearch store");

            var response = await _client.Indices.DeleteAsync(indexName).ConfigureAwait(false);
            if (response.IsValid == false)
                throw new PolyPersistException($"Failed to drop index '{indexName}': {response.ServerError?.Error?.Reason ?? response.DebugInformation}");
        }

        // Bridges the class-constraint gap: OpenSearch.Client requires `class`, the interface only
        // guarantees ISearchDocument, new(). At runtime TDocument is always a class, so the
        // (class-constrained) index is built via reflection.
        private ISearchIndex<TDocument> _NewIndex<TDocument>(string indexName) where TDocument : ISearchDocument, new()
        {
            var type = typeof(OpenSearch_SearchIndex<>).MakeGenericType(typeof(TDocument));
            return (ISearchIndex<TDocument>)Activator.CreateInstance(type, indexName, this)!;
        }
    }
}
