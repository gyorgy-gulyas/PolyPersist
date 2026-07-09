using System.Collections.Concurrent;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.SearchStore.Memory
{
    /// <summary>
    /// In-process search store. It is the Docker-free baseline for the portable search surface
    /// (index / delete / simple full-text search with paging) and the test reference the other
    /// backends must match; it is not meant for production-scale relevance.
    /// </summary>
    public class Memory_SearchStore : ISearchStore
    {
        // indexName -> the (boxed) Memory_SearchIndex<TDocument> created for it.
        internal readonly ConcurrentDictionary<string, object> _indexes = new();

        public Memory_SearchStore(string connectionString)
        {
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Search;
        /// <inheritdoc/>
        string IStore.ProviderName => "Memory_Search";

        /// <inheritdoc/>
        Task<bool> ISearchStore.IsIndexExists(string indexName)
            => Task.FromResult(_indexes.ContainsKey(indexName));

        /// <inheritdoc/>
        Task<ISearchIndex<TDocument>> ISearchStore.GetIndexByName<TDocument>(string indexName)
        {
            if (_indexes.TryGetValue(indexName, out var index) == false)
                throw new NotFoundException($"Index '{indexName}' does not exist in Memory Search Store");

            return Task.FromResult((ISearchIndex<TDocument>)index);
        }

        /// <inheritdoc/>
        Task<ISearchIndex<TDocument>> ISearchStore.CreateIndex<TDocument>(string indexName)
        {
            var index = new Memory_SearchIndex<TDocument>(indexName, this);
            if (_indexes.TryAdd(indexName, index) == false)
                throw new DuplicateKeyException($"Index '{indexName}' already exists in Memory Search Store");

            return Task.FromResult((ISearchIndex<TDocument>)index);
        }

        /// <inheritdoc/>
        Task ISearchStore.DropIndex(string indexName)
        {
            if (_indexes.TryRemove(indexName, out _) == false)
                throw new NotFoundException($"Index '{indexName}' does not exist in Memory Search Store");

            return Task.CompletedTask;
        }
    }
}
