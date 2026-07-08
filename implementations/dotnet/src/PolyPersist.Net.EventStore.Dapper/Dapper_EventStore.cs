using System.Data.Common;
using Dapper;
using Microsoft.Data.Sqlite;
using Npgsql;
using PolyPersist.Net.Core;

namespace PolyPersist.Net.EventStore.Dapper
{
    /// <summary>
    /// Append-only, immutable event/stream store on a SQL backend, using pure Dapper (no LINQ needed:
    /// the surface is append + read-by-stream). A single <c>events</c> table with a composite primary
    /// key <c>(streamId, version)</c> gives optimistic concurrency for free (a duplicate version is a
    /// conflict). Works on SQLite and PostgreSQL; the provider is chosen at construction.
    ///
    /// Event sourcing and audit are both just events on streams (the store does not distinguish them).
    /// Subscriptions/projections are provider-specific and reached via GetUnderlyingImplementation().
    /// </summary>
    public class Dapper_EventStore : IEventStore
    {
        private readonly string _provider;
        private readonly string _connectionString;
        private readonly string _table;
        private int _ensured;

        // Sentinels for expectedVersion (mirror the contract).
        private const int Any = -2;
        private const int NoStream = -1;

        /// <param name="provider">"SQLite.MS" or "PostgreSQL".</param>
        /// <param name="connectionString">the ADO.NET connection string for that provider.</param>
        /// <param name="tableName">the events table name (default "events").</param>
        public Dapper_EventStore(string provider, string connectionString, string tableName = "events")
        {
            _provider = provider;
            _connectionString = connectionString;
            _table = tableName;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.EventStore;
        /// <inheritdoc/>
        string IStore.ProviderName => $"EventStore.Dapper({_provider})";

        private DbConnection _NewConnection() => _provider switch
        {
            "SQLite.MS" or "SQLite" or "Sqlite" => new SqliteConnection(_connectionString),
            "PostgreSQL" or "Postgres" or "Npgsql" => new NpgsqlConnection(_connectionString),
            _ => throw new NotSupportedException($"Unsupported event-store provider '{_provider}'")
        };

        private async Task<DbConnection> _OpenAsync()
        {
            var conn = _NewConnection();
            await conn.OpenAsync().ConfigureAwait(false);
            await _EnsureTableAsync(conn).ConfigureAwait(false);
            return conn;
        }

        private async Task _EnsureTableAsync(DbConnection conn)
        {
            if (Volatile.Read(ref _ensured) == 1)
                return;

            string ddl = $@"CREATE TABLE IF NOT EXISTS ""{_table}"" (
    ""streamId""  TEXT NOT NULL,
    ""version""   INTEGER NOT NULL,
    ""eventId""   TEXT,
    ""eventType"" TEXT,
    ""data""      TEXT,
    ""metadata""  TEXT,
    ""timestamp"" TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (""streamId"", ""version"")
)";
            await conn.ExecuteAsync(ddl).ConfigureAwait(false);
            Volatile.Write(ref _ensured, 1);
        }

        private async Task<int> _CurrentVersionAsync(DbConnection conn, string streamId, DbTransaction tx = null)
        {
            long? max = await conn.ExecuteScalarAsync<long?>(
                $@"SELECT MAX(""version"") FROM ""{_table}"" WHERE ""streamId"" = @s",
                new { s = streamId }, tx).ConfigureAwait(false);
            return max.HasValue ? (int)max.Value : NoStream;
        }

        private static void _GuardExpectedVersion(string streamId, int expectedVersion, int currentVersion)
        {
            if (expectedVersion == Any)
                return;
            if (expectedVersion == NoStream && currentVersion != NoStream)
                throw new Exception($"Concurrency conflict: stream '{streamId}' already exists (version {currentVersion})");
            if (expectedVersion >= 0 && currentVersion != expectedVersion)
                throw new Exception($"Concurrency conflict: stream '{streamId}' expected version {expectedVersion} but was {currentVersion}");
        }

        /// <inheritdoc/>
        async Task<int> IEventStore.AppendToStream(string streamId, int expectedVersion, IList<IEvent> events)
        {
            using var conn = await _OpenAsync().ConfigureAwait(false);
            using var tx = await conn.BeginTransactionAsync().ConfigureAwait(false);

            int current = await _CurrentVersionAsync(conn, streamId, tx as DbTransaction).ConfigureAwait(false);
            _GuardExpectedVersion(streamId, expectedVersion, current);

            int version = current;
            foreach (var e in events)
            {
                version++;
                e.streamId = streamId;
                e.version = version;
                if (string.IsNullOrEmpty(e.eventId) == true)
                    e.eventId = Guid.NewGuid().ToString();
                if (e.timestamp == default)
                    e.timestamp = DateTime.UtcNow;

                await conn.ExecuteAsync(
                    $@"INSERT INTO ""{_table}"" (""streamId"",""version"",""eventId"",""eventType"",""data"",""metadata"",""timestamp"")
                       VALUES (@streamId,@version,@eventId,@eventType,@data,@metadata,@timestamp)",
                    new { e.streamId, e.version, e.eventId, e.eventType, e.data, e.metadata, e.timestamp },
                    tx as DbTransaction).ConfigureAwait(false);
            }

            await tx.CommitAsync().ConfigureAwait(false);
            return version;
        }

        /// <inheritdoc/>
        async Task<IList<IEvent>> IEventStore.ReadStream(string streamId, int fromVersion, int maxCount)
        {
            using var conn = await _OpenAsync().ConfigureAwait(false);
            string limit = maxCount >= 0 ? $" LIMIT {maxCount}" : string.Empty;

            var rows = await conn.QueryAsync<Event>(
                $@"SELECT ""streamId"",""version"",""eventId"",""eventType"",""data"",""metadata"",""timestamp""
                   FROM ""{_table}"" WHERE ""streamId"" = @s AND ""version"" >= @from ORDER BY ""version""{limit}",
                new { s = streamId, from = fromVersion }).ConfigureAwait(false);

            return rows.Cast<IEvent>().ToList();
        }

        /// <inheritdoc/>
        async Task<bool> IEventStore.StreamExists(string streamId)
        {
            using var conn = await _OpenAsync().ConfigureAwait(false);
            long? one = await conn.ExecuteScalarAsync<long?>(
                $@"SELECT 1 FROM ""{_table}"" WHERE ""streamId"" = @s LIMIT 1",
                new { s = streamId }).ConfigureAwait(false);
            return one.HasValue;
        }

        /// <inheritdoc/>
        async Task<int> IEventStore.GetStreamVersion(string streamId)
        {
            using var conn = await _OpenAsync().ConfigureAwait(false);
            return await _CurrentVersionAsync(conn, streamId).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IEventStore.DeleteStream(string streamId, int expectedVersion)
        {
            using var conn = await _OpenAsync().ConfigureAwait(false);
            int current = await _CurrentVersionAsync(conn, streamId).ConfigureAwait(false);
            _GuardExpectedVersion(streamId, expectedVersion, current);

            await conn.ExecuteAsync(
                $@"DELETE FROM ""{_table}"" WHERE ""streamId"" = @s",
                new { s = streamId }).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        object IEventStore.GetUnderlyingImplementation()
        {
            // A fresh (unopened) ADO connection for provider-specific use (raw SQL, polling-based
            // subscriptions, ...). The caller opens and disposes it.
            return _NewConnection();
        }
    }
}
