using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using PolyPersist.Net.AnalyticalStore.GCPBigQuery;
using PolyPersist.Net.AnalyticalStore.ClickHouse;
using PolyPersist.Net.AnalyticalStore.Postgres;
using Testcontainers.ClickHouse;
using Testcontainers.PostgreSql;

namespace PolyPersist.Net.AnalyticalStore.Tests
{
    // ---- Test fact records (denormalized rows; a PartitionKey routing column, but no id / etag) ----
    // PartitionKey defaults to "p1" so the cross-partition aggregate fixtures need not set it; the
    // partition-scoping tests assign explicit partitions.

    /// <summary>A sales fact row: dimensions (Region, Product, SoldAt) + measures (Quantity, Amount).</summary>
    public class Sale : IAnalyticalRecord
    {
        public string PartitionKey { get; set; } = "p1";
        public string Region { get; set; } = null!;
        public string Product { get; set; } = null!;
        public int Quantity { get; set; }
        public decimal Amount { get; set; }
        public DateTime SoldAt { get; set; }
    }

    /// <summary>A fact row exercising the remaining column types (bool, double, long, nullable, Guid).</summary>
    public class Metric : IAnalyticalRecord
    {
        public string PartitionKey { get; set; } = "p1";
        public string Name { get; set; } = null!;
        public bool Flag { get; set; }
        public double Ratio { get; set; }
        public long Big { get; set; }
        public int? Score { get; set; }
        public Guid Ref { get; set; }
        public DateTime At { get; set; }
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

        private static PostgreSqlContainer _pg = null!;
        private static readonly SemaphoreSlim _pgLock = new(1, 1);

        static TestMain()
        {
            _Setup_Postgres();
            _Setup_ClickHouse();
            _Setup_BigQuery();
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

        private static ClickHouseContainer _ch = null!;
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

        private static IContainer _bq = null!;
        private static BigQueryClient _bqClient = null!;
        private static readonly SemaphoreSlim _bqLock = new(1, 1);
        private const string _bqDataset = "ds";

        // Exposed so BigQuery-provider-specific tests (e.g. unsupported operators) can target it directly.
        public static Func<string, Task<IAnalyticalStore>> BigQueryFactory { get; private set; } = null!;

        private static void _Setup_BigQuery()
        {
            Func<string, Task<IAnalyticalStore>> factory = async _ =>
            {
                await _EnsureBigQuery().ConfigureAwait(false);
                return new BigQuery_AnalyticalStore(_bqClient, _bqDataset);
            };
            BigQueryFactory = factory;
            StoreInstances.Add([factory]);
        }

        private static async Task _EnsureBigQuery()
        {
            if (_bqClient != null)
                return;

            await _bqLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_bqClient != null)
                    return;

                var container = new ContainerBuilder()
                    .WithImage("ghcr.io/goccy/bigquery-emulator:latest")
                    .WithCommand("--project=test", $"--dataset={_bqDataset}")
                    .WithPortBinding(9050, assignRandomHostPort: true)
                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9050))
                    .Build();

                await container.StartAsync().ConfigureAwait(false);
                _bq = container;

                _bqClient = new BigQueryClientBuilder
                {
                    ProjectId = "test",
                    BaseUri = $"http://{_bq.Hostname}:{_bq.GetMappedPublicPort(9050)}",
                    Credential = GoogleCredential.FromAccessToken("dummy-token"),
                }.Build();
            }
            finally
            {
                _bqLock.Release();
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
            if (_bq != null)
                await _bq.DisposeAsync().ConfigureAwait(false);
        }
    }
}
