using Cassandra;
using PolyPersist.Net.Core;

namespace PolyPersist.Net.EventStore.Cassandra
{
    /// <summary>
    /// Append-only event/stream store on Cassandra/Scylla. A stream is a partition (streamid), events
    /// are clustered by version ascending, so per-stream ordering and the (streamid, version) uniqueness
    /// come from the primary key. Optimistic concurrency uses a read-guard for clear messages plus an
    /// <c>INSERT ... IF NOT EXISTS</c> (LWT) as the ultimate guard against races.
    /// </summary>
    public class Cassandra_EventStore : IEventStore
    {
        private const int Any = -2;
        private const int NoStream = -1;

        private readonly ISession _session;
        private readonly string _keyspace;
        private readonly string _table;
        private int _ensured;

        public Cassandra_EventStore(string connectionString, string tableName = "events")
        {
            var cfg = _Parse(connectionString);
            var cluster = Cluster.Builder()
                .AddContactPoint(cfg.Host)
                .WithPort(cfg.Port)
                .WithCredentials(cfg.Username, cfg.Password)
                .Build();

            _session = cluster.Connect(cfg.Keyspace);
            _keyspace = cfg.Keyspace;
            _table = tableName;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.EventStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "EventStore.Cassandra";

        private async Task _EnsureTableAsync()
        {
            if (Volatile.Read(ref _ensured) == 1)
                return;

            await _session.ExecuteAsync(new SimpleStatement(
                $@"CREATE TABLE IF NOT EXISTS {_keyspace}.{_table} (
                    streamid text,
                    version int,
                    eventid text,
                    eventtype text,
                    data text,
                    metadata text,
                    timestamp timestamp,
                    PRIMARY KEY (streamid, version)
                ) WITH CLUSTERING ORDER BY (version ASC)")).ConfigureAwait(false);

            Volatile.Write(ref _ensured, 1);
        }

        private async Task<int> _CurrentVersionAsync(string streamId)
        {
            var rs = await _session.ExecuteAsync(new SimpleStatement(
                $"SELECT MAX(version) AS m FROM {_keyspace}.{_table} WHERE streamid = ?", streamId)).ConfigureAwait(false);
            var row = rs.FirstOrDefault();
            int? v = row?.GetValue<int?>("m");
            return v ?? NoStream;
        }

        private static void _Guard(string streamId, int expectedVersion, int currentVersion)
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
            await _EnsureTableAsync().ConfigureAwait(false);

            int current = await _CurrentVersionAsync(streamId).ConfigureAwait(false);
            _Guard(streamId, expectedVersion, current);

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

                var ts = new DateTimeOffset(DateTime.SpecifyKind(e.timestamp, DateTimeKind.Utc));
                var rs = await _session.ExecuteAsync(new SimpleStatement(
                    $@"INSERT INTO {_keyspace}.{_table} (streamid,version,eventid,eventtype,data,metadata,timestamp)
                       VALUES (?,?,?,?,?,?,?) IF NOT EXISTS",
                    e.streamId, e.version, e.eventId, e.eventType, e.data, e.metadata, ts)).ConfigureAwait(false);

                bool applied = rs.FirstOrDefault()?.GetValue<bool>("[applied]") ?? true;
                if (applied == false)
                    throw new Exception($"Concurrency conflict: stream '{streamId}' version {version} already exists");
            }

            return version;
        }

        /// <inheritdoc/>
        async Task<IList<IEvent>> IEventStore.ReadStream(string streamId, int fromVersion, int maxCount)
        {
            await _EnsureTableAsync().ConfigureAwait(false);

            string cql = $"SELECT streamid,version,eventid,eventtype,data,metadata,timestamp FROM {_keyspace}.{_table} WHERE streamid = ? AND version >= ?";
            if (maxCount >= 0)
                cql += $" LIMIT {maxCount}";

            var rs = await _session.ExecuteAsync(new SimpleStatement(cql, streamId, fromVersion)).ConfigureAwait(false);

            var list = new List<IEvent>();
            foreach (var row in rs)
            {
                list.Add(new Event
                {
                    streamId = row.GetValue<string>("streamid"),
                    version = row.GetValue<int>("version"),
                    eventId = row.GetValue<string>("eventid"),
                    eventType = row.GetValue<string>("eventtype"),
                    data = row.GetValue<string>("data"),
                    metadata = row.GetValue<string>("metadata"),
                    timestamp = row.GetValue<DateTimeOffset>("timestamp").UtcDateTime,
                });
            }
            return list;
        }

        /// <inheritdoc/>
        async Task<bool> IEventStore.StreamExists(string streamId)
        {
            await _EnsureTableAsync().ConfigureAwait(false);
            var rs = await _session.ExecuteAsync(new SimpleStatement(
                $"SELECT streamid FROM {_keyspace}.{_table} WHERE streamid = ? LIMIT 1", streamId)).ConfigureAwait(false);
            return rs.Any();
        }

        /// <inheritdoc/>
        async Task<int> IEventStore.GetStreamVersion(string streamId)
        {
            await _EnsureTableAsync().ConfigureAwait(false);
            return await _CurrentVersionAsync(streamId).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task IEventStore.DeleteStream(string streamId, int expectedVersion)
        {
            await _EnsureTableAsync().ConfigureAwait(false);
            int current = await _CurrentVersionAsync(streamId).ConfigureAwait(false);
            _Guard(streamId, expectedVersion, current);

            await _session.ExecuteAsync(new SimpleStatement(
                $"DELETE FROM {_keyspace}.{_table} WHERE streamid = ?", streamId)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        object IEventStore.GetUnderlyingImplementation() => _session;

        private record struct _Conn(string Host, int Port, string Username, string Password, string Keyspace);

        private static _Conn _Parse(string connectionString)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var kv = part.Split('=', 2);
                if (kv.Length == 2)
                    dict[kv[0].Trim()] = kv[1].Trim();
            }
            dict.TryGetValue("host", out var host);
            dict.TryGetValue("port", out var port);
            dict.TryGetValue("username", out var username);
            dict.TryGetValue("password", out var password);
            dict.TryGetValue("keyspace", out var keyspace);
            return new _Conn(host, int.TryParse(port, out var p) ? p : 9042, username, password, keyspace);
        }
    }
}
