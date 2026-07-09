using PolyPersist.Net.Core;
using PolyPersist.Net.RelationalStore.Dapper;
using PolyPersist.Net.Test;
using Testcontainers.PostgreSql;

namespace PolyPersist.Net.RelationalStore.Tests
{
    // ---- Test records (flat, scalar columns) ----

    public class SampleRecord : Entity, IRecord
    {
        public string Name { get; set; } = null!;
        public int Age { get; set; }
        public decimal Balance { get; set; }
    }

    public class Customer : Entity, IRecord
    {
        public string Name { get; set; } = null!;
    }

    public class Order : Entity, IRecord
    {
        public string CustomerId { get; set; } = null!;
        public decimal Total { get; set; }
    }

    /// <summary>
    /// Registers the relational store backends the tests run against:
    /// - SQLite: an isolated temp-file database per store (always available, no Docker).
    /// - PostgreSQL: a single Testcontainer, started lazily the first time a test pulls it.
    /// </summary>
    [TestClass]
    public static class TestMain
    {
        // Each entry is a factory: (unused-name) -> a fresh IRelationalStore.
        public static List<object[]> StoreInstances { get; } = [];

        private static readonly List<string> _sqliteFiles = [];

        private static PostgreSqlContainer _pg = null!;
        private static readonly SemaphoreSlim _pgLock = new(1, 1);

        static TestMain()
        {
            _Setup_Sqlite();
            _Setup_Postgres();
        }

        // Helper used by the tests to obtain a globally-unique, storage-conform table name.
        public static string NewTableName() => ("t" + Guid.NewGuid().ToString("N")).MakeStorageConformName();

        private static void _Setup_Sqlite()
        {
            Func<string, Task<IRelationalStore>> factory = _ =>
            {
                // SQLite ':memory:' is per-connection; the store opens a connection per operation,
                // so a temp file is used to keep the database across those connections.
                string file = Path.Combine(Path.GetTempPath(), $"pp_rel_{Guid.NewGuid():N}.db");
                lock (_sqliteFiles)
                    _sqliteFiles.Add(file);

                IRelationalStore store = new Relational_Store("SQLite.MS", $"Data Source={file}");
                return Task.FromResult(store);
            };
            StoreInstances.Add([factory]);
        }

        private static void _Setup_Postgres()
        {
            Func<string, Task<IRelationalStore>> factory = async _ =>
            {
                await _EnsurePostgres().ConfigureAwait(false);
                return new Relational_Store("PostgreSQL", _pg.GetConnectionString());
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

        [AssemblyInitialize]
        public static Task AssemblyInit(TestContext _) => Task.CompletedTask;

        [AssemblyCleanup]
        public static async Task Cleanup()
        {
            if (_pg != null)
                await _pg.DisposeAsync().ConfigureAwait(false);

            lock (_sqliteFiles)
            {
                foreach (var file in _sqliteFiles)
                {
                    try
                    {
                        if (File.Exists(file))
                            File.Delete(file);
                    }
                    catch { /* best-effort temp cleanup */ }
                }
            }
        }
    }
}
