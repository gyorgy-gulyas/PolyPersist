using PolyPersist.Net.Common;
using StackExchange.Redis;

namespace PolyPersist.Net.CacheStore.RespProtocol
{
    /// <summary>
    /// Distributed key-value cache over the RESP protocol. ONE implementation serves the whole RESP
    /// family - Valkey (the recommended default: BSD-3, Linux Foundation), Redis, Garnet, Dragonfly -
    /// and their managed equivalents, because they all speak the same wire protocol and this store
    /// only uses the commands (GET / SET / EXPIRE / EXISTS / DEL) that every one of them implements.
    /// The server is chosen by the connection string, exactly as the relational store chooses its SQL
    /// dialect by provider name.
    /// <para>
    /// Provider-specific capabilities the thin cache surface does not expose (atomic counters,
    /// pub/sub, native data structures) are reachable through
    /// <see cref="ICacheStore.GetUnderlyingImplementation"/>, which hands back the
    /// <see cref="IDatabase"/>.
    /// </para>
    /// </summary>
    public class Resp_CacheStore : ICacheStore, IDisposable
    {
        private readonly ConnectionMultiplexer _connection;
        private readonly IDatabase _database;
        private readonly string _keyPrefix;
        private bool _disposed;

        /// <param name="connectionString">a StackExchange.Redis configuration string, e.g. "localhost:6379".</param>
        /// <param name="keyPrefix">
        /// prepended to every key. A cache server is often shared between applications, and the RESP
        /// keyspace is flat; the prefix is what keeps two tenants of one server apart.
        /// </param>
        /// <param name="database">the numbered database to use, or -1 for the connection's default.</param>
        public Resp_CacheStore(string connectionString, string keyPrefix = "", int database = -1)
        {
            _connection = ConnectionMultiplexer.Connect(connectionString);
            _database = _connection.GetDatabase(database);
            _keyPrefix = keyPrefix ?? string.Empty;
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.Cache;
        /// <inheritdoc/>
        string IStore.ProviderName => "Resp_Cache";

        /// <inheritdoc/>
        async Task ICacheStore.Set<T>(string key, T value, int ttlSeconds)
        {
            _CheckKey(key);

            // A null expiry both means "no expiration" and CLEARS any time-to-live a previous value
            // under this key carried, which is what ttlSeconds <= 0 has to mean.
            TimeSpan? expiry = ttlSeconds > 0 ? TimeSpan.FromSeconds(ttlSeconds) : null;

            await _database.StringSetAsync(_Key(key), CacheValue.Serialize(value), expiry).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<T> ICacheStore.Get<T>(string key)
        {
            _CheckKey(key);

            RedisValue value = await _database.StringGetAsync(_Key(key)).ConfigureAwait(false);

            // An absent OR expired key reads as a null RedisValue; both mean "not cached".
            return value.IsNull ? default! : CacheValue.Deserialize<T>(value!);
        }

        /// <inheritdoc/>
        async Task<bool> ICacheStore.Exists(string key)
        {
            _CheckKey(key);
            return await _database.KeyExistsAsync(_Key(key)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ICacheStore.Remove(string key)
        {
            _CheckKey(key);
            await _database.KeyDeleteAsync(_Key(key)).ConfigureAwait(false);   // false = absent, a no-op
        }

        /// <inheritdoc/>
        /// <remarks>
        /// The caller must NOT dispose the returned database: it belongs to this store's connection,
        /// which serves every other operation. The multiplexer is reachable via IDatabase.Multiplexer.
        /// </remarks>
        object ICacheStore.GetUnderlyingImplementation() => _database;

        private RedisKey _Key(string key) => _keyPrefix + key;

        private static void _CheckKey(string key)
        {
            if (string.IsNullOrEmpty(key) == true)
                throw new InvalidRequestException("Cache key must not be null or empty");
        }

        public void Dispose()
        {
            if (_disposed == true)
                return;

            _disposed = true;
            _connection.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
