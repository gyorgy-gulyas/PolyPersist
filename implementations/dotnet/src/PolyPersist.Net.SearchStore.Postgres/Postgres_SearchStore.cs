using Npgsql;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.SearchStore.Postgres
{
    /// <summary>
    /// Full-text search store on PostgreSQL (tsvector / tsquery). Each index is a table
    /// (id, doc jsonb, txt, tsv tsvector GENERATED ... STORED with a GIN index). The portable surface
    /// (index / delete / simple full-text search with paging) is served in-database; anything richer
    /// (custom ranking, facets, highlighting) is reached via GetUnderlyingImplementation. Good for
    /// moderate needs where a dedicated search cluster is not warranted.
    /// </summary>
    public class Postgres_SearchStore : ISearchStore
    {
        private readonly string _connectionString;

        /// <param name="connectionString">the Npgsql connection string.</param>
        public Postgres_SearchStore(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Search;
        /// <inheritdoc/>
        string IStore.ProviderName => "Search(PostgreSQL)";

        // Short-lived connection for a single operation; the underlying ADO connection is pooled.
        internal NpgsqlConnection OpenConnection()
        {
            var conn = new NpgsqlConnection(_connectionString);
            conn.Open();
            return conn;
        }

        internal async Task<NpgsqlConnection> OpenConnectionAsync()
        {
            var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync().ConfigureAwait(false);
            return conn;
        }

        /// <inheritdoc/>
        async Task<bool> ISearchStore.IsIndexExists(string indexName)
        {
            // Probe the table directly: a SELECT against a missing table throws (schema metadata can
            // lag a just-created table).
            await using var conn = await OpenConnectionAsync().ConfigureAwait(false);
            try
            {
                await using var cmd = new NpgsqlCommand($"SELECT 1 FROM \"{indexName}\" WHERE 1 = 0", conn);
                await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                return true;
            }
            catch (PostgresException)
            {
                return false;
            }
        }

        /// <inheritdoc/>
        async Task<ISearchIndex<TDocument>> ISearchStore.GetIndexByName<TDocument>(string indexName)
        {
            if (await ((ISearchStore)this).IsIndexExists(indexName).ConfigureAwait(false) == false)
                throw new NotFoundException($"Index '{indexName}' does not exist in the PostgreSQL search store");

            return new Postgres_SearchIndex<TDocument>(indexName, this);
        }

        /// <inheritdoc/>
        async Task<ISearchIndex<TDocument>> ISearchStore.CreateIndex<TDocument>(string indexName)
        {
            if (await ((ISearchStore)this).IsIndexExists(indexName).ConfigureAwait(false) == true)
                throw new DuplicateKeyException($"Index '{indexName}' already exists in the PostgreSQL search store");

            await using var conn = await OpenConnectionAsync().ConfigureAwait(false);

            // 'txt' holds the concatenated searchable text (built by the index on write); 'tsv' is a
            // stored generated tsvector over it, GIN-indexed for fast @@ matching.
            string ddl =
                $"CREATE TABLE \"{indexName}\" (" +
                "id text PRIMARY KEY, " +
                "doc jsonb NOT NULL, " +
                "txt text NOT NULL, " +
                "tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', txt)) STORED)";
            await using (var cmd = new NpgsqlCommand(ddl, conn))
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            await using (var cmd = new NpgsqlCommand($"CREATE INDEX ON \"{indexName}\" USING GIN (tsv)", conn))
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            // pg_trgm trigram index powers Fuzzy mode (substring ILIKE + word_similarity typo match).
            await using (var cmd = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_trgm", conn))
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            await using (var cmd = new NpgsqlCommand($"CREATE INDEX ON \"{indexName}\" USING GIN (txt gin_trgm_ops)", conn))
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);

            return new Postgres_SearchIndex<TDocument>(indexName, this);
        }

        /// <inheritdoc/>
        async Task ISearchStore.DropIndex(string indexName)
        {
            if (await ((ISearchStore)this).IsIndexExists(indexName).ConfigureAwait(false) == false)
                throw new NotFoundException($"Index '{indexName}' does not exist in the PostgreSQL search store");

            await using var conn = await OpenConnectionAsync().ConfigureAwait(false);
            await using var cmd = new NpgsqlCommand($"DROP TABLE \"{indexName}\"", conn);
            await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
}
