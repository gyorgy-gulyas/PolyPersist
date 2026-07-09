using System.Reflection;
using System.Text.Json;
using Npgsql;

namespace PolyPersist.Net.SearchStore.Postgres
{
    /// <summary>
    /// A single PostgreSQL full-text index (one table). Documents are upserted by id (the full
    /// document is stored as jsonb for reconstruction; its public string properties are concatenated
    /// into the indexed 'txt'). <see cref="Search"/> runs websearch_to_tsquery + ts_rank in-database.
    /// </summary>
    internal class Postgres_SearchIndex<TDocument> : ISearchIndex<TDocument>
        where TDocument : ISearchDocument, new()
    {
        // Public readable string properties form the free-text that gets indexed.
        private static readonly PropertyInfo[] _textProps = typeof(TDocument)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(string) && p.CanRead)
            .ToArray();

        private readonly string _name;
        private readonly Postgres_SearchStore _store;

        public Postgres_SearchIndex(string name, Postgres_SearchStore store)
        {
            _name = name;
            _store = store;
        }

        /// <inheritdoc/>
        string ISearchIndex<TDocument>.Name => _name;
        /// <inheritdoc/>
        IStore ISearchIndex<TDocument>.ParentStore => _store;

        private static string _SearchableText(TDocument document)
            => string.Join(' ', _textProps.Select(p => p.GetValue(document) as string ?? string.Empty));

        /// <inheritdoc/>
        async Task ISearchIndex<TDocument>.Index(TDocument document)
        {
            await using var conn = await _store.OpenConnectionAsync().ConfigureAwait(false);
            await _UpsertAsync(conn, document, null).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ISearchIndex<TDocument>.IndexBatch(IList<TDocument> documents)
        {
            if (documents == null || documents.Count == 0)
                return;

            await using var conn = await _store.OpenConnectionAsync().ConfigureAwait(false);
            await using var tx = await conn.BeginTransactionAsync().ConfigureAwait(false);
            foreach (var document in documents)
                await _UpsertAsync(conn, document, tx).ConfigureAwait(false);
            await tx.CommitAsync().ConfigureAwait(false);
        }

        private async Task _UpsertAsync(NpgsqlConnection conn, TDocument document, NpgsqlTransaction? tx)
        {
            string sql =
                $"INSERT INTO \"{_name}\" (id, doc, txt) VALUES (@id, @doc::jsonb, @txt) " +
                "ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc, txt = EXCLUDED.txt";
            await using var cmd = new NpgsqlCommand(sql, conn, tx);
            cmd.Parameters.AddWithValue("id", document.id);
            cmd.Parameters.AddWithValue("doc", JsonSerializer.Serialize(document));
            cmd.Parameters.AddWithValue("txt", _SearchableText(document));
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ISearchIndex<TDocument>.Delete(string id)
        {
            await using var conn = await _store.OpenConnectionAsync().ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand($"DELETE FROM \"{_name}\" WHERE id = @id", conn);
            cmd.Parameters.AddWithValue("id", id);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<IList<TDocument>> ISearchIndex<TDocument>.Search(string queryText, SearchMode mode, int from, int size)
        {
            // FullText: whole-word tsquery + ts_rank. Fuzzy: substring (ILIKE) unioned with trigram
            // word_similarity (typo tolerance), both served by the pg_trgm GIN index. The 'id'
            // tie-break gives a stable order so paging (LIMIT/OFFSET) is deterministic.
            string sql = mode == SearchMode.FullText
                ? $"SELECT doc FROM \"{_name}\" " +
                  "WHERE tsv @@ websearch_to_tsquery('english', @q) " +
                  "ORDER BY ts_rank(tsv, websearch_to_tsquery('english', @q)) DESC, id " +
                  "LIMIT @size OFFSET @from"
                : $"SELECT doc FROM \"{_name}\" " +
                  "WHERE txt ILIKE '%' || @q || '%' OR word_similarity(@q, txt) > 0.3 " +
                  "ORDER BY word_similarity(@q, txt) DESC, id " +
                  "LIMIT @size OFFSET @from";

            await using var conn = await _store.OpenConnectionAsync().ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("q", queryText ?? string.Empty);
            cmd.Parameters.AddWithValue("size", size < 0 ? 0 : size);
            cmd.Parameters.AddWithValue("from", from < 0 ? 0 : from);

            var result = new List<TDocument>();
            await using var reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                string json = reader.GetFieldValue<string>(0);
                var document = JsonSerializer.Deserialize<TDocument>(json);
                if (document != null)
                    result.Add(document);
            }
            return result;
        }

        /// <inheritdoc/>
        object ISearchIndex<TDocument>.GetUnderlyingImplementation() => _store.OpenConnection();
    }
}
