using System.Text;
using EventStore.Client;
using PolyPersist.Net.Core;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.EventStore.EventStoreDB
{
    /// <summary>
    /// Append-only event/stream store on EventStoreDB (native gRPC client). Streams, revisions and
    /// optimistic concurrency (expected revision) are first-class in the engine, so the contract maps
    /// almost directly. Subscriptions/projections are reached via GetUnderlyingImplementation()
    /// (the native EventStoreClient).
    /// </summary>
    public class EventStoreDB_EventStore : IEventStore
    {
        private const int Any = -2;
        private const int NoStream = -1;

        private readonly EventStoreClient _client;

        public EventStoreDB_EventStore(string connectionString)
        {
            var settings = EventStoreClientSettings.Create(connectionString);
            _client = new EventStoreClient(settings);
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.EventStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "EventStore.EventStoreDB";

        /// <inheritdoc/>
        async Task<int> IEventStore.AppendToStream(string streamId, int expectedVersion, IList<IEvent> events)
        {
            var payload = new List<EventData>();
            foreach (var e in events)
            {
                e.streamId = streamId;
                if (string.IsNullOrEmpty(e.eventId) == true || Guid.TryParse(e.eventId, out _) == false)
                    e.eventId = Guid.NewGuid().ToString();
                if (e.timestamp == default)
                    e.timestamp = DateTime.UtcNow;

                ReadOnlyMemory<byte>? meta = string.IsNullOrEmpty(e.metadata)
                    ? null
                    : Encoding.UTF8.GetBytes(e.metadata);

                payload.Add(new EventData(
                    Uuid.FromGuid(Guid.Parse(e.eventId)),
                    string.IsNullOrEmpty(e.eventType) ? "event" : e.eventType,
                    Encoding.UTF8.GetBytes(e.data ?? string.Empty),
                    meta));
            }

            try
            {
                IWriteResult result = expectedVersion switch
                {
                    Any => await _client.AppendToStreamAsync(streamId, StreamState.Any, payload).ConfigureAwait(false),
                    NoStream => await _client.AppendToStreamAsync(streamId, StreamState.NoStream, payload).ConfigureAwait(false),
                    _ => await _client.AppendToStreamAsync(streamId, StreamRevision.FromInt64(expectedVersion), payload).ConfigureAwait(false),
                };
                return (int)result.NextExpectedStreamRevision.ToInt64();
            }
            catch (WrongExpectedVersionException)
            {
                if (expectedVersion == NoStream)
                    throw new PolyPersist.Net.Common.DuplicateKeyException($"Concurrency conflict: stream '{streamId}' already exists");
                throw new ConcurrencyConflictException($"Concurrency conflict: stream '{streamId}' expected version {expectedVersion}");
            }
        }

        /// <inheritdoc/>
        async Task<IList<IEvent>> IEventStore.ReadStream(string streamId, int fromVersion, int maxCount)
        {
            long count = maxCount >= 0 ? maxCount : long.MaxValue;
            var result = _client.ReadStreamAsync(Direction.Forwards, streamId, StreamPosition.FromInt64(fromVersion), count);

            if (await result.ReadState.ConfigureAwait(false) == ReadState.StreamNotFound)
                return [];

            var list = new List<IEvent>();
            await foreach (var resolved in result.ConfigureAwait(false))
            {
                var ev = resolved.Event;
                list.Add(new Event
                {
                    streamId = streamId,
                    version = (int)ev.EventNumber.ToInt64(),
                    eventId = ev.EventId.ToString(),
                    eventType = ev.EventType,
                    data = Encoding.UTF8.GetString(ev.Data.ToArray()),
                    metadata = ev.Metadata.Length > 0 ? Encoding.UTF8.GetString(ev.Metadata.ToArray()) : null!,
                    timestamp = ev.Created,
                });
            }
            return list;
        }

        /// <inheritdoc/>
        async Task<bool> IEventStore.StreamExists(string streamId)
        {
            var result = _client.ReadStreamAsync(Direction.Forwards, streamId, StreamPosition.Start, 1);
            return await result.ReadState.ConfigureAwait(false) != ReadState.StreamNotFound;
        }

        /// <inheritdoc/>
        async Task<int> IEventStore.GetStreamVersion(string streamId)
        {
            var result = _client.ReadStreamAsync(Direction.Backwards, streamId, StreamPosition.End, 1);
            if (await result.ReadState.ConfigureAwait(false) == ReadState.StreamNotFound)
                return NoStream;

            await foreach (var resolved in result.ConfigureAwait(false))
                return (int)resolved.Event.EventNumber.ToInt64();

            return NoStream;
        }

        /// <inheritdoc/>
        async Task IEventStore.DeleteStream(string streamId, int expectedVersion)
        {
            try
            {
                _ = expectedVersion switch
                {
                    Any => await _client.DeleteAsync(streamId, StreamState.Any).ConfigureAwait(false),
                    NoStream => await _client.DeleteAsync(streamId, StreamState.NoStream).ConfigureAwait(false),
                    _ => await _client.DeleteAsync(streamId, StreamRevision.FromInt64(expectedVersion)).ConfigureAwait(false),
                };
            }
            catch (WrongExpectedVersionException)
            {
                throw new ConcurrencyConflictException($"Concurrency conflict: stream '{streamId}' expected version {expectedVersion}");
            }
        }

        /// <inheritdoc/>
        object IEventStore.GetUnderlyingImplementation() => _client;
    }
}
