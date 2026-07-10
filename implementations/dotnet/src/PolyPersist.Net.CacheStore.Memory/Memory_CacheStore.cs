using System.Collections.Concurrent;
using PolyPersist.Net.Common;

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
    /// not accumulate forever. There is no size-bounded (LRU) eviction.
    /// </para>
    /// </summary>
    public class Memory_CacheStore : ICacheStore
    {
        private readonly record struct _Entry(string Json, DateTimeOffset? ExpiresAt);

        private readonly ConcurrentDictionary<string, _Entry> _entries = new();
        private readonly TimeProvider _time;

        // Amortises the cost of reclaiming never-read expired keys over many writes.
        private const int _SweepEveryWrites = 256;
        private int _writesSinceSweep;

        /// <param name="connectionString">unused; kept for symmetry with the other stores.</param>
        /// <param name="timeProvider">the clock; injectable so expiration can be tested deterministically.</param>
        public Memory_CacheStore(string connectionString, TimeProvider? timeProvider = null)
        {
            _time = timeProvider ?? TimeProvider.System;
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

            _entries[key] = new _Entry(CacheValue.Serialize(value), expiresAt);

            if (Interlocked.Increment(ref _writesSinceSweep) >= _SweepEveryWrites)
            {
                Interlocked.Exchange(ref _writesSinceSweep, 0);
                _SweepExpired();
            }

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

            if (_entries.TryGetValue(key, out _Entry entry) == false)
                return false;

            if (_IsExpired(entry) == true)
            {
                _entries.TryRemove(key, out _);
                return false;
            }

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

        private static void _CheckKey(string key)
        {
            if (string.IsNullOrEmpty(key) == true)
                throw new InvalidRequestException("Cache key must not be null or empty");
        }
    }
}
