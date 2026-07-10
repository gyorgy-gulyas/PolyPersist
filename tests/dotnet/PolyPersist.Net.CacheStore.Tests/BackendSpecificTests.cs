using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using PolyPersist.Net.CacheStore.Memory;
using PolyPersist.Net.CacheStore.RespProtocol;
using StackExchange.Redis;

namespace PolyPersist.Net.CacheStore.Tests
{
    /// <summary>A clock the test drives, so expiration is asserted without waiting for real time.</summary>
    [ExcludeFromCodeCoverage]
    internal sealed class TestTimeProvider : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 7, 10, 8, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now = _now.Add(by);
    }

    [TestClass]
    public class MemoryCacheStoreTests
    {
        // The contract suite has to wait for real seconds to pass; here the clock is ours.
        [TestMethod]
        public async Task Ttl_ExpiresExactlyWhenTheClockPassesIt()
        {
            var clock = new TestTimeProvider();
            ICacheStore cache = new Memory_CacheStore("", clock);

            await cache.Set("k", "v", 10);

            clock.Advance(TimeSpan.FromSeconds(9));
            Assert.IsTrue(await cache.Exists("k"));
            Assert.AreEqual("v", await cache.Get<string>("k"));

            clock.Advance(TimeSpan.FromSeconds(1));
            Assert.IsFalse(await cache.Exists("k"), "the entry expires the moment its TTL is reached");
            Assert.IsNull(await cache.Get<string>("k"));
        }

        // Keys nobody reads again must not accumulate. Reading them would evict them lazily and hide
        // the bug, so the backing table is inspected directly.
        [TestMethod]
        public async Task ExpiredEntries_AreSweptByLaterWrites_WithoutBeingRead()
        {
            var clock = new TestTimeProvider();
            ICacheStore cache = new Memory_CacheStore("", clock);
            var table = (IDictionary)cache.GetUnderlyingImplementation();

            for (int i = 0; i < 100; i++)
                await cache.Set($"expiring{i}", i, 10);

            clock.Advance(TimeSpan.FromSeconds(11));
            Assert.AreEqual(100, table.Count, "expired but not yet reclaimed");

            // A sweep runs every 256 writes; these 100 expired keys are never read.
            for (int i = 0; i < 256; i++)
                await cache.Set($"live{i}", i, 0);

            Assert.AreEqual(256, table.Count, "only the live entries survive the sweep");
        }

        [TestMethod]
        public void GetUnderlyingImplementation_IsTheBackingTable()
        {
            ICacheStore cache = new Memory_CacheStore("");

            object table = cache.GetUnderlyingImplementation();

            Assert.AreEqual(typeof(ConcurrentDictionary<,>), table.GetType().GetGenericTypeDefinition());
        }
    }

    [TestClass]
    public class RespCacheStoreTests
    {
        // The RESP keyspace is flat, so two applications on one server are kept apart by the prefix.
        [TestMethod]
        public async Task KeyPrefix_IsolatesTwoStoresOnTheSameServer()
        {
            string connectionString = await TestMain.ValkeyConnectionString();
            using var tenantA = new Resp_CacheStore(connectionString, keyPrefix: "tenantA:");
            using var tenantB = new Resp_CacheStore(connectionString, keyPrefix: "tenantB:");

            await ((ICacheStore)tenantA).Set("shared", "belongs to A", 0);

            Assert.AreEqual("belongs to A", await ((ICacheStore)tenantA).Get<string>("shared"));
            Assert.IsFalse(await ((ICacheStore)tenantB).Exists("shared"));
            Assert.IsNull(await ((ICacheStore)tenantB).Get<string>("shared"));
        }

        // The escape hatch: everything the thin surface does not expose lives on the native client.
        [TestMethod]
        public async Task GetUnderlyingImplementation_ExposesTheNativeDatabase()
        {
            string connectionString = await TestMain.ValkeyConnectionString();
            using var store = new Resp_CacheStore(connectionString, keyPrefix: Guid.NewGuid().ToString("N") + ":");

            var database = (IDatabase)((ICacheStore)store).GetUnderlyingImplementation();

            // an atomic counter - a capability the portable cache surface deliberately does not have
            string key = "counter" + Guid.NewGuid().ToString("N");
            Assert.AreEqual(1, await database.StringIncrementAsync(key));
            Assert.AreEqual(2, await database.StringIncrementAsync(key));
            await database.KeyDeleteAsync(key);
        }
    }
}
