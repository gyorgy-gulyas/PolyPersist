using System.Collections.Concurrent;
using PolyPersist.Net.Common;
using PolyPersist.Net.Core;

namespace PolyPersist.Net.CacheStore.Memory
{
    /// <summary>
    /// In-process key-value cache with a time-to-live. It serves both as an L1 cache in front of a
    /// distributed one and as the Docker-free reference the RESP backends must match.
    /// <para>
    /// Values are stored SERIALIZED, not as object references. Handing the caller back the very
    /// instance it cached would let a later mutation of that instance silently rewrite what everyone
    /// else reads - the same reason <c>Memory_DocumentCollection</c> and <c>Memory_ColumnTable</c>
    /// serialize. It also keeps this store an honest stand-in for a real cache across the wire.
    /// </para>
    /// <para>
    /// Expiration is lazy: an entry is dropped when it is next read. Writes additionally sweep the
    /// whole table every <see cref="_SweepEveryWrites"/> calls, so keys that are never read again do
    /// not accumulate forever.
    /// </para>
    /// <para>
    /// Time alone is not enough of a bound, because an entry written without a TTL never expires
    /// (PP-58). The store therefore also holds at most <c>maxEntries</c> keys and evicts the least
    /// recently used ones once it is full. Recency is approximate by design: reads only stamp a
    /// counter on the entry, they take no lock, so under concurrency two entries stamped at nearly
    /// the same moment may be evicted in either order. That is the standard trade in an LRU cache -
    /// a wrong eviction costs one recomputation, never correctness. The RESP backends have no such
    /// parameter: bounding a shared server is its own <c>maxmemory-policy</c> setting, not ours.
    /// </para>
    /// </summary>
    public class Memory_CacheStore : ICacheStore
    {
        // A class, not a record struct: LastAccess is stamped in place on every read, and rewriting
        // a whole struct back into the dictionary just to record a read would be both slower and
        // racier (it could resurrect a value a concurrent Set had already replaced).
        private sealed class _Entry(string json, DateTimeOffset? expiresAt, long lastAccess)
        {
            public string Json { get; } = json;
            public DateTimeOffset? ExpiresAt { get; } = expiresAt;
            public long LastAccess = lastAccess;
        }

        private readonly ConcurrentDictionary<string, _Entry> _entries = new();
        private readonly TimeProvider _time;

        // Amortises the cost of reclaiming never-read expired keys over many writes.
        private const int _SweepEveryWrites = 256;
        private int _writesSinceSweep;

        /// <summary>Default upper bound on the number of cached keys; 0 would mean unbounded.</summary>
        public const int DefaultMaxEntries = 10_000;

        private readonly int _maxEntries;
        // Monotonic tick used to order entries by recency. It only ever has to be comparable, so it
        // is a counter rather than a clock (which could stand still or, in tests, be rewound).
        private long _accessTick;

        /// <param name="connectionString">unused; kept for symmetry with the other stores.</param>
        /// <param name="timeProvider">the clock; injectable so expiration can be tested deterministically.</param>
        /// <param name="maxEntries">
        /// the most keys to hold before least-recently-used eviction starts; pass 0 for an unbounded
        /// cache (only sensible when the key space is known to be small and the process short-lived).
        /// </param>
        public Memory_CacheStore(string connectionString, TimeProvider? timeProvider = null, int maxEntries = DefaultMaxEntries)
        {
            if (maxEntries < 0)
                throw new InvalidRequestException("Cache maxEntries must not be negative (0 means unbounded)");

            _time = timeProvider ?? TimeProvider.System;
            _maxEntries = maxEntries;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Cache;
        /// <inheritdoc/>
        string IStore.ProviderName => "Memory_Cache";

        /// <inheritdoc/>
        Task ICacheStore.Set<T>(string key, T value, int ttlSeconds)
        {
            _CheckKey(key);

            DateTimeOffset? expiresAt = ttlSeconds > 0
                ? _time.GetUtcNow().AddSeconds(ttlSeconds)
                : null;   // ttlSeconds <= 0 means no expiration

            _entries[key] = new _Entry(CacheValue.Serialize(value), expiresAt, Interlocked.Increment(ref _accessTick));

            if (Interlocked.Increment(ref _writesSinceSweep) >= _SweepEveryWrites)
            {
                Interlocked.Exchange(ref _writesSinceSweep, 0);
                _SweepExpired();
            }

            _EnforceCapacity();

            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        Task<T> ICacheStore.Get<T>(string key)
        {
            _CheckKey(key);

            return Task.FromResult(_TryRead(key, out string? json)
                ? CacheValue.Deserialize<T>(json!)
                : default!);
        }

        /// <inheritdoc/>
        Task<ICacheEntry<T>> ICacheStore.TryGet<T>(string key)
        {
            _CheckKey(key);

            return Task.FromResult(_TryRead(key, out string? json)
                ? CacheEntry<T>.Hit(CacheValue.Deserialize<T>(json!))
                : CacheEntry<T>.Miss);
        }

        /// <inheritdoc/>
        Task<bool> ICacheStore.Exists(string key)
        {
            _CheckKey(key);
            return Task.FromResult(_TryRead(key, out _));
        }

        /// <inheritdoc/>
        Task ICacheStore.Remove(string key)
        {
            _CheckKey(key);
            _entries.TryRemove(key, out _);   // absent key: a no-op, per the contract
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        object ICacheStore.GetUnderlyingImplementation() => _entries;

        // Reads a live entry, dropping it if its time-to-live has passed.
        private bool _TryRead(string key, out string? json)
        {
            json = null;

            if (_entries.TryGetValue(key, out _Entry? entry) == false)
                return false;

            if (_IsExpired(entry!) == true)
            {
                _entries.TryRemove(key, out _);
                return false;
            }

            // A hit makes the entry the most recently used one, which is what keeps a hot key alive
            // while the cache evicts around it.
            Interlocked.Exchange(ref entry!.LastAccess, Interlocked.Increment(ref _accessTick));

            json = entry.Json;
            return true;
        }

        private bool _IsExpired(_Entry entry)
            => entry.ExpiresAt is not null && entry.ExpiresAt <= _time.GetUtcNow();

        private void _SweepExpired()
        {
            foreach (var pair in _entries)
            {
                if (_IsExpired(pair.Value) == true)
                    _entries.TryRemove(pair.Key, out _);
            }
        }

        /// <summary>
        /// Drops the least recently used entries once the table is over its bound. Expired entries
        /// go first - they cost nothing to lose. What remains is cut back to nine tenths of the
        /// limit rather than to the limit itself, so the ordering scan runs once every few hundred
        /// writes instead of on every single write past the bound.
        /// </summary>
        private void _EnforceCapacity()
        {
            if (_maxEntries <= 0 || _entries.Count <= _maxEntries)
                return;

            _SweepExpired();

            int lowWaterMark = _maxEntries - (_maxEntries / 10);
            int toEvict = _entries.Count - lowWaterMark;
            if (toEvict <= 0)
                return;

            foreach (var pair in _entries.OrderBy(p => Interlocked.Read(ref p.Value.LastAccess)).Take(toEvict))
                _entries.TryRemove(pair.Key, out _);
        }

        private static void _CheckKey(string key)
        {
            if (string.IsNullOrEmpty(key) == true)
                throw new InvalidRequestException("Cache key must not be null or empty");
        }
    }
}
