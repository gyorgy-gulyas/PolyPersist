using System.Diagnostics.CodeAnalysis;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PolyPersist.Net.CacheStore.Memory;
using PolyPersist.Net.CacheStore.RespProtocol;

namespace PolyPersist.Net.CacheStore.Tests
{
    public enum Colour { Red, Green, Blue }

    /// <summary>A value with enough shape to catch a serialization drift between backends.</summary>
    [ExcludeFromCodeCoverage]
    public class CacheDto
    {
        public string Name { get; set; } = null!;
        public int Age { get; set; }
        public Colour Favourite { get; set; }
        public DateTime Stamp { get; set; }
    }

    /// <summary>
    /// Registers every cache backend the contract tests run against. Each entry is a factory that
    /// returns a fresh <see cref="ICacheStore"/>.
    /// <para>
    /// Memory is always available (Docker-free). The RESP store is registered TWICE - once against
    /// Valkey and once against Redis - because it claims to be one implementation for the whole RESP
    /// family; running the same contract suite against both servers is what turns that claim into a
    /// verified fact. Each RESP store instance gets a unique key prefix, so the tests sharing one
    /// container cannot collide.
    /// </para>
    /// </summary>
    [TestClass]
    public static class TestMain
    {
        private const int _RespPort = 6379;

        // Each entry: [ Func<Task<ICacheStore>> ].
        public static List<object[]> StoreInstances { get; } = [];

        static TestMain()
        {
            _Setup_Memory();
            _Setup_Resp("valkey/valkey:8-alpine", () => _valkey, c => _valkey = c, _valkeyLock);
            _Setup_Resp("redis:7-alpine", () => _redis, c => _redis = c, _redisLock);
        }

        private static void _Setup_Memory()
        {
            Func<Task<ICacheStore>> factory = () => Task.FromResult<ICacheStore>(new Memory_CacheStore(""));
            StoreInstances.Add([factory]);
        }

        private static IContainer? _valkey;
        private static readonly SemaphoreSlim _valkeyLock = new(1, 1);
        private static IContainer? _redis;
        private static readonly SemaphoreSlim _redisLock = new(1, 1);

        private static void _Setup_Resp(string image, Func<IContainer?> get, Action<IContainer> set, SemaphoreSlim gate)
        {
            Func<Task<ICacheStore>> factory = async () =>
            {
                IContainer container = await _EnsureContainer(image, get, set, gate).ConfigureAwait(false);
                string connectionString = $"{container.Hostname}:{container.GetMappedPublicPort(_RespPort)}";

                // A unique prefix per store: the container is shared by every test in the run.
                return new Resp_CacheStore(connectionString, keyPrefix: Guid.NewGuid().ToString("N") + ":");
            };
            StoreInstances.Add([factory]);
        }

        private static async Task<IContainer> _EnsureContainer(string image, Func<IContainer?> get, Action<IContainer> set, SemaphoreSlim gate)
        {
            if (get() != null)
                return get()!;

            await gate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (get() != null)
                    return get()!;

                var container = new ContainerBuilder()
                    .WithImage(image)
                    .WithPortBinding(_RespPort, assignRandomHostPort: true)
                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(_RespPort))
                    .WithCleanUp(true)
                    .Build();

                await container.StartAsync().ConfigureAwait(false);
                set(container);
                return container;
            }
            finally
            {
                gate.Release();
            }
        }

        /// <summary>A RESP connection string, for the tests that need two stores on one server.</summary>
        public static async Task<string> ValkeyConnectionString()
        {
            var container = await _EnsureContainer("valkey/valkey:8-alpine", () => _valkey, c => _valkey = c, _valkeyLock).ConfigureAwait(false);
            return $"{container.Hostname}:{container.GetMappedPublicPort(_RespPort)}";
        }

        [AssemblyCleanup]
        public static async Task Cleanup()
        {
            if (_valkey != null)
                await _valkey.DisposeAsync().ConfigureAwait(false);

            if (_redis != null)
                await _redis.DisposeAsync().ConfigureAwait(false);
        }
    }
}
