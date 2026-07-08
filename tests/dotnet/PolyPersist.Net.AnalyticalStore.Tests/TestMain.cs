using PolyPersist.Net.AnalyticalStore.ClickHouse;
using PolyPersist.Net.AnalyticalStore.Postgres;
using Testcontainers.ClickHouse;
using Testcontainers.PostgreSql;

namespace PolyPersist.Net.AnalyticalStore.Tests
{
    // ---- Test fact records (denormalized rows; no id / etag / partition key) ----

    /// <summary>A sales fact row: dimensions (Region, Product, SoldAt) + measures (Quantity, Amount).</summary>
    public class Sale : IAnalyticalRecord
    {
        public string Region { get; set; }
        public string Product { get; set; }
        public int Quantity { get; set; }
        public decimal Amount { get; set; }
        public DateTime SoldAt { get; set; }
    }

    /// <summary>
    /// Registers the analytical (OLAP) backends the tests run against. The factory ignores its
    /// argument and returns a fresh store; each test creates its own uniquely-named fact table so
    /// tests are isolated even on a shared container.
    /// - PostgreSQL: a single Testcontainer, started lazily on first use.
    /// (ClickHouse and DuckDB are added alongside once their impl projects exist.)
    /// </summary>
    [TestClass]
    public static class TestMain
    {
        // Each entry is a factory: (unused) -> a fresh IAnalyticalStore on the shared backend.
        public static List<object[]> StoreInstances { get; } = [];

        private static PostgreSqlContainer _pg;
        private static readonly SemaphoreSlim _pgLock = new(1, 1);

        static TestMain()
        {
            _Setup_Postgres();
            _Setup_ClickHouse();
        }

        // Globally-unique, storage-conform fact-table name (lower-case hex, starts with a letter).
        public static string NewTableName() => "t" + Guid.NewGuid().ToString("N");

        private static void _Setup_Postgres()
        {
            Func<string, Task<IAnalyticalStore>> factory = async _ =>
            {
                await _EnsurePostgres().ConfigureAwait(false);
                return new Postgres_AnalyticalStore(_pg.GetConnectionString());
            };
            StoreInstances.Add([factory]);
        }

        private static async Task _EnsurePostgres()
        {
            if (_pg != null)
                return;

            await _pgLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_pg != null)
                    return;

                var container = new PostgreSqlBuilder()
                    .WithImage("postgres:16-alpine")
                    .WithCleanUp(true)
                    .Build();

                await container.StartAsync().ConfigureAwait(false);
                _pg = container;
            }
            finally
            {
                _pgLock.Release();
            }
        }

        private static ClickHouseContainer _ch;
        private static readonly SemaphoreSlim _chLock = new(1, 1);

        private static void _Setup_ClickHouse()
        {
            Func<string, Task<IAnalyticalStore>> factory = async _ =>
            {
                await _EnsureClickHouse().ConfigureAwait(false);
                return new ClickHouse_AnalyticalStore(_ch.GetConnectionString());
            };
            StoreInstances.Add([factory]);
        }

        private static async Task _EnsureClickHouse()
        {
            if (_ch != null)
                return;

            await _chLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_ch != null)
                    return;

                var container = new ClickHouseBuilder()
                    .WithCleanUp(true)
                    .Build();

                await container.StartAsync().ConfigureAwait(false);
                _ch = container;
            }
            finally
            {
                _chLock.Release();
            }
        }

        [AssemblyInitialize]
        public static Task AssemblyInit(TestContext _) => Task.CompletedTask;

        [AssemblyCleanup]
        public static async Task Cleanup()
        {
            if (_pg != null)
                await _pg.DisposeAsync().ConfigureAwait(false);
            if (_ch != null)
                await _ch.DisposeAsync().ConfigureAwait(false);
        }
    }
}
