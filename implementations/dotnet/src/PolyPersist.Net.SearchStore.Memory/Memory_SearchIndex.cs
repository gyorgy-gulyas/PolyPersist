using System.Reflection;

namespace PolyPersist.Net.SearchStore.Memory
{
    /// <summary>
    /// A single in-memory search index. Documents are upserted by id; <see cref="Search"/> is a naive
    /// term-frequency scorer over the document's public string properties (the same "searchable text"
    /// idea the SQL/engine backends implement natively). Only the portable surface is meaningful here;
    /// engine-specific relevance/facets are not modelled.
    /// </summary>
    internal class Memory_SearchIndex<TDocument> : ISearchIndex<TDocument>
        where TDocument : ISearchDocument, new()
    {
        // Public readable string properties form the free-text haystack.
        private static readonly PropertyInfo[] _textProps = typeof(TDocument)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(string) && p.CanRead)
            .ToArray();

        private static readonly char[] _separators =
            { ' ', '\t', '\n', '\r', '.', ',', ';', ':', '!', '?', '"', '\'', '(', ')', '[', ']', '{', '}', '/', '\\', '-' };

        private readonly string _name;
        private readonly Memory_SearchStore _store;
        private readonly Dictionary<string, TDocument> _docs = new();
        private readonly object _lock = new();

        public Memory_SearchIndex(string name, Memory_SearchStore store)
        {
            _name = name;
            _store = store;
        }

        /// <inheritdoc/>
        string ISearchIndex<TDocument>.Name => _name;
        /// <inheritdoc/>
        IStore ISearchIndex<TDocument>.ParentStore => _store;

        /// <inheritdoc/>
        Task ISearchIndex<TDocument>.Index(TDocument document)
        {
            lock (_lock)
                _docs[document.id] = document;
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task ISearchIndex<TDocument>.IndexBatch(IList<TDocument> documents)
        {
            lock (_lock)
            {
                foreach (var document in documents)
                    _docs[document.id] = document;
            }
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task ISearchIndex<TDocument>.Delete(string id)
        {
            lock (_lock)
                _docs.Remove(id);
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<IList<TDocument>> ISearchIndex<TDocument>.Search(string queryText, SearchMode mode, int from, int size)
        {
            string[] terms = _Tokenize(queryText);

            List<TDocument> ranked;
            lock (_lock)
            {
                ranked = _docs.Values
                    .Select(doc => (doc, score: _Score(doc, terms, mode)))
                    .Where(x => x.score > 0)
                    .OrderByDescending(x => x.score)
                    .Select(x => x.doc)
                    .ToList();
            }

            IList<TDocument> page = ranked
                .Skip(from < 0 ? 0 : from)
                .Take(size < 0 ? 0 : size)
                .ToList();
            return Task.FromResult(page);
        }

        /// <inheritdoc/>
        object ISearchIndex<TDocument>.GetUnderlyingImplementation() => _docs;

        private static string[] _Tokenize(string? text)
            => (text ?? string.Empty).ToLowerInvariant().Split(_separators, StringSplitOptions.RemoveEmptyEntries);

        // Score = sum of term frequencies of the query terms in the document's text (OR semantics:
        // any matching term contributes). In FullText a token must equal the term; in Fuzzy it may
        // contain the term as a fragment or be within a small edit distance (typo). Multi-term boolean
        // logic and real relevance are engine-specific and live behind GetUnderlyingImplementation.
        private static int _Score(TDocument doc, string[] terms, SearchMode mode)
        {
            if (terms.Length == 0)
                return 0;

            string haystack = string.Join(' ', _textProps.Select(p => p.GetValue(doc) as string ?? string.Empty));
            string[] tokens = _Tokenize(haystack);

            int score = 0;
            foreach (string term in terms)
            {
                if (mode == SearchMode.FullText)
                    score += tokens.Count(t => t == term);
                else
                    score += tokens.Count(t => _FuzzyMatch(t, term));
            }
            return score;
        }

        // Loose match: the token contains the term as a fragment, or is within a small edit distance.
        private static bool _FuzzyMatch(string token, string term)
            => token.Contains(term, StringComparison.Ordinal) || _Levenshtein(token, term) <= _FuzzyThreshold(term);

        // Mirrors OpenSearch "AUTO" fuzziness: 0 edits for 1-2 chars, 1 for 3-5, 2 for 6+.
        private static int _FuzzyThreshold(string term)
            => term.Length <= 2 ? 0 : term.Length <= 5 ? 1 : 2;

        private static int _Levenshtein(string a, string b)
        {
            int[] prev = new int[b.Length + 1];
            int[] curr = new int[b.Length + 1];
            for (int j = 0; j <= b.Length; j++)
                prev[j] = j;

            for (int i = 1; i <= a.Length; i++)
            {
                curr[0] = i;
                for (int j = 1; j <= b.Length; j++)
                {
                    int cost = a[i - 1] == b[j - 1] ? 0 : 1;
                    curr[j] = Math.Min(Math.Min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
                }
                (prev, curr) = (curr, prev);
            }
            return prev[b.Length];
        }
    }
}
