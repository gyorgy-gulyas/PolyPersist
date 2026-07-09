using OpenSearch.Client;
using OpenSearch.Net;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.SearchStore.OpenSearch
{
    /// <summary>
    /// A single OpenSearch index. Documents are upserted by id (the whole document is the indexed
    /// _source); writes force a refresh so they are immediately searchable (the tests, and typical
    /// read-after-write callers, expect that). <see cref="Search"/> is a simple free-text query over
    /// all fields, relevance-ordered.
    /// </summary>
    internal class OpenSearch_SearchIndex<TDocument> : ISearchIndex<TDocument>
        where TDocument : class, ISearchDocument, new()
    {
        private readonly string _name;
        private readonly OpenSearch_SearchStore _store;

        private OpenSearchClient _client => _store._client;

        public OpenSearch_SearchIndex(string name, OpenSearch_SearchStore store)
        {
            _name = name;
            _store = store;
        }

        /// <inheritdoc/>
        string ISearchIndex<TDocument>.Name => _name;
        /// <inheritdoc/>
        IStore ISearchIndex<TDocument>.ParentStore => _store;

        /// <inheritdoc/>
        async Task ISearchIndex<TDocument>.Index(TDocument document)
        {
            var response = await _client
                .IndexAsync(document, i => i.Index(_name).Id(document.id).Refresh(Refresh.True))
                .ConfigureAwait(false);
            if (response.IsValid == false)
                throw new PolyPersistException($"Index failed: {response.ServerError?.Error?.Reason ?? response.DebugInformation}");
        }

        /// <inheritdoc/>
        async Task ISearchIndex<TDocument>.IndexBatch(IList<TDocument> documents)
        {
            if (documents == null || documents.Count == 0)
                return;

            var response = await _client
                .BulkAsync(b => b
                    .Index(_name)
                    .IndexMany(documents, (op, document) => op.Id(document.id))
                    .Refresh(Refresh.True))
                .ConfigureAwait(false);
            if (response.IsValid == false)
                throw new PolyPersistException($"IndexBatch failed: {response.ServerError?.Error?.Reason ?? response.DebugInformation}");
        }

        /// <inheritdoc/>
        async Task ISearchIndex<TDocument>.Delete(string id)
        {
            var response = await _client
                .DeleteAsync<TDocument>(id, d => d.Index(_name).Refresh(Refresh.True))
                .ConfigureAwait(false);
            // A missing document is not an error for an idempotent delete.
            if (response.IsValid == false && response.Result != Result.NotFound)
                throw new PolyPersistException($"Delete failed: {response.ServerError?.Error?.Reason ?? response.DebugInformation}");
        }

        /// <inheritdoc/>
        async Task<IList<TDocument>> ISearchIndex<TDocument>.Search(string queryText, SearchMode mode, int from, int size)
        {
            // FullText: simple_query_string (forgiving free text over all fields, whole-word).
            // Fuzzy: union of a substring wildcard (*term*) and a fuzziness=AUTO match (typo tolerance).
            // Advanced querying goes through GetUnderlyingImplementation.
            QueryContainer query;
            if (mode == SearchMode.FullText)
            {
                query = new SimpleQueryStringQuery { Query = queryText };
            }
            else
            {
                query = new BoolQuery
                {
                    Should = new QueryContainer[]
                    {
                        new QueryStringQuery { Query = $"*{queryText}*" },
                        new MultiMatchQuery { Query = queryText, Fuzziness = Fuzziness.Auto },
                    },
                    MinimumShouldMatch = 1,
                };
            }

            var response = await _client
                .SearchAsync<TDocument>(s => s
                    .Index(_name)
                    .From(from < 0 ? 0 : from)
                    .Size(size < 0 ? 0 : size)
                    .Query(_ => query))
                .ConfigureAwait(false);
            if (response.IsValid == false)
                throw new PolyPersistException($"Search failed: {response.ServerError?.Error?.Reason ?? response.DebugInformation}");

            return response.Documents.ToList();
        }

        /// <inheritdoc/>
        object ISearchIndex<TDocument>.GetUnderlyingImplementation() => _client;
    }
}
