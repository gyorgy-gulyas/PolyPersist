using PolyPersist.Net.Core;

namespace PolyPersist.Net.EventStore.Memory
{
    /// <summary>
    /// In-process append-only event/stream store. Not durable - for tests and for consumers' unit
    /// tests of event-sourced aggregates without a database. Stores copies of events so external
    /// mutation cannot corrupt stored state (mirrors the persistence semantics of the real stores).
    /// </summary>
    public class Memory_EventStore : IEventStore
    {
        private const int Any = -2;
        private const int NoStream = -1;

        private readonly Dictionary<string, List<IEvent>> _streams = [];
        private readonly object _lock = new();

        public Memory_EventStore(string? connectionString = null)
        {
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.EventStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "EventStore.Memory";

        private static IEvent _Clone(IEvent e) => new Event
        {
            eventId = e.eventId,
            streamId = e.streamId,
            version = e.version,
            eventType = e.eventType,
            data = e.data,
            metadata = e.metadata,
            timestamp = e.timestamp,
        };

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
        Task<int> IEventStore.AppendToStream(string streamId, int expectedVersion, IList<IEvent> events)
        {
            lock (_lock)
            {
                _streams.TryGetValue(streamId, out var list);
                int current = (list?.Count ?? 0) - 1;
                _Guard(streamId, expectedVersion, current);

                if (list == null)
                {
                    list = [];
                    _streams[streamId] = list;
                }

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

                    list.Add(_Clone(e));
                }

                return Task.FromResult(version);
            }
        }

        /// <inheritdoc/>
        Task<IList<IEvent>> IEventStore.ReadStream(string streamId, int fromVersion, int maxCount)
        {
            lock (_lock)
            {
                if (_streams.TryGetValue(streamId, out var list) == false)
                    return Task.FromResult<IList<IEvent>>([]);

                IEnumerable<IEvent> query = list.Where(e => e.version >= fromVersion);
                if (maxCount >= 0)
                    query = query.Take(maxCount);

                return Task.FromResult<IList<IEvent>>(query.Select(_Clone).ToList());
            }
        }

        /// <inheritdoc/>
        Task<bool> IEventStore.StreamExists(string streamId)
        {
            lock (_lock)
                return Task.FromResult(_streams.TryGetValue(streamId, out var list) && list.Count > 0);
        }

        /// <inheritdoc/>
        Task<int> IEventStore.GetStreamVersion(string streamId)
        {
            lock (_lock)
                return Task.FromResult(_streams.TryGetValue(streamId, out var list) ? list.Count - 1 : NoStream);
        }

        /// <inheritdoc/>
        Task IEventStore.DeleteStream(string streamId, int expectedVersion)
        {
            lock (_lock)
            {
                _streams.TryGetValue(streamId, out var list);
                int current = (list?.Count ?? 0) - 1;
                _Guard(streamId, expectedVersion, current);

                _streams.Remove(streamId);
                return Task.CompletedTask;
            }
        }

        /// <inheritdoc/>
        object IEventStore.GetUnderlyingImplementation()
        {
            lock (_lock)
                return _streams;
        }
    }
}
