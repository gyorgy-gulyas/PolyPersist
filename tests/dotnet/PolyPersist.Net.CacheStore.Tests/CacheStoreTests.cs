using PolyPersist.Net.Common;

namespace PolyPersist.Net.CacheStore.Tests
{
    /// <summary>
    /// The portable ICacheStore contract, asserted by RESULT against every backend: Memory, Valkey
    /// and Redis must be indistinguishable through this surface.
    /// </summary>
    [TestClass]
    public class CacheStoreTests
    {
        private static string Key() => "k" + Guid.NewGuid().ToString("N");

        private static CacheDto Dto() => new()
        {
            Name = "Alice",
            Age = 30,
            Favourite = Colour.Green,
            Stamp = new DateTime(2026, 7, 10, 8, 0, 0, DateTimeKind.Utc),
        };

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_Get_String_RoundTrips(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set(key, "hello", 0);

            Assert.AreEqual("hello", await cache.Get<string>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_Get_Int_RoundTrips(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set(key, 42, 0);

            Assert.AreEqual(42, await cache.Get<int>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_Get_Dto_RoundTrips(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();
            var dto = Dto();

            await cache.Set(key, dto, 0);
            var read = await cache.Get<CacheDto>(key);

            Assert.AreEqual(dto.Name, read.Name);
            Assert.AreEqual(dto.Age, read.Age);
            Assert.AreEqual(dto.Favourite, read.Favourite);
            Assert.AreEqual(dto.Stamp, read.Stamp);
        }

        // The cache must hand back a COPY: mutating what was cached may not change what others read.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_Get_Dto_ReturnsACopy_NotTheCachedInstance(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();
            var dto = Dto();

            await cache.Set(key, dto, 0);
            dto.Name = "mutated after caching";

            var read = await cache.Get<CacheDto>(key);
            Assert.AreEqual("Alice", read.Name);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_Get_List_RoundTrips(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set(key, new List<int> { 1, 2, 3 }, 0);

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, await cache.Get<List<int>>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_Get_Dictionary_RoundTrips(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set(key, new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 }, 0);
            var read = await cache.Get<Dictionary<string, int>>(key);

            Assert.AreEqual(2, read.Count);
            Assert.AreEqual(1, read["a"]);
            Assert.AreEqual(2, read["b"]);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Get_MissingKey_ReturnsNull(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();

            Assert.IsNull(await cache.Get<CacheDto>(Key()));
        }

        // Documents a known limitation of the contract: for a VALUE type, a miss and a cached default
        // are indistinguishable through Get<T>. Exists() is the (racy, two round-trip) way to tell.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Get_MissingKey_ValueType_ReturnsDefault_NotDistinguishableFromCachedZero(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string missing = Key();
            string cachedZero = Key();
            await cache.Set(cachedZero, 0, 0);

            Assert.AreEqual(0, await cache.Get<int>(missing));
            Assert.AreEqual(0, await cache.Get<int>(cachedZero));

            Assert.IsFalse(await cache.Exists(missing));
            Assert.IsTrue(await cache.Exists(cachedZero));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_Overwrites_ExistingValue(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set(key, "first", 0);
            await cache.Set(key, "second", 0);

            Assert.AreEqual("second", await cache.Get<string>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Exists_ReflectsPresence(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            Assert.IsFalse(await cache.Exists(key));
            await cache.Set(key, "x", 0);
            Assert.IsTrue(await cache.Exists(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Remove_DeletesValue(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();
            await cache.Set(key, "x", 0);

            await cache.Remove(key);

            Assert.IsFalse(await cache.Exists(key));
            Assert.IsNull(await cache.Get<string>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Remove_MissingKey_IsANoOp(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();

            await cache.Remove(Key());   // must not throw
        }

        // ttlSeconds <= 0 means "no expiration"; it must also CLEAR a time-to-live an earlier write set.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_WithNonPositiveTtl_DoesNotExpire_AndClearsAPreviousTtl(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set(key, "x", 1);
            await cache.Set(key, "y", 0);

            await Task.Delay(TimeSpan.FromMilliseconds(1500));

            Assert.IsTrue(await cache.Exists(key), "the second write must have cleared the 1s TTL");
            Assert.AreEqual("y", await cache.Get<string>(key));
        }

        // The contract's TTL is in whole seconds, so this waits for real time to pass. The Memory
        // backend also has a deterministic clock-driven test (MemoryCacheStoreTests).
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_WithTtl_ExpiresAfterTheTtlPasses(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set(key, "x", 1);
            Assert.IsTrue(await cache.Exists(key));

            bool expired = false;
            for (int attempt = 0; attempt < 20 && expired == false; attempt++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250));
                expired = await cache.Exists(key) == false;
            }

            Assert.IsTrue(expired, "the entry should have expired within 5s of its 1s TTL");
            Assert.IsNull(await cache.Get<string>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Set_NullValue_IsCached_AndReadsBackAsNull(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();

            await cache.Set<CacheDto?>(key, null, 0);

            Assert.IsTrue(await cache.Exists(key), "a cached null is a present entry");
            Assert.IsNull(await cache.Get<CacheDto?>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task LargeValue_RoundTrips(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();
            string key = Key();
            string payload = new('x', 1024 * 1024);

            await cache.Set(key, payload, 0);

            Assert.AreEqual(payload, await cache.Get<string>(key));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task EmptyKey_Throws(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();

            await Assert.ThrowsExceptionAsync<InvalidRequestException>(() => cache.Set("", "x", 0));
            await Assert.ThrowsExceptionAsync<InvalidRequestException>(() => cache.Get<string>(""));
            await Assert.ThrowsExceptionAsync<InvalidRequestException>(() => cache.Exists(""));
            await Assert.ThrowsExceptionAsync<InvalidRequestException>(() => cache.Remove(""));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task StorageModel_Is_Cache(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();

            Assert.AreEqual(IStore.StorageModels.Cache, ((IStore)cache).StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(((IStore)cache).ProviderName));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GetUnderlyingImplementation_IsAvailable(Func<Task<ICacheStore>> factory)
        {
            var cache = await factory();

            // Deliberately NOT disposed: for the RESP store it is the shared connection's database.
            Assert.IsNotNull(cache.GetUnderlyingImplementation());
        }
    }
}
