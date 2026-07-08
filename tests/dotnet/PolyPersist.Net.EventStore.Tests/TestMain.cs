using PolyPersist.Net.EventStore.Dapper;
using PolyPersist.Net.Test;
using Testcontainers.PostgreSql;

namespace PolyPersist.Net.EventStore.Tests
{
    /// <summary>
    /// Registers the event-store backends the tests run against. The factory argument is a unique
    /// events-table name (per test) so tests are isolated even on the shared PostgreSQL container.
    /// - SQLite: an isolated temp-file database per store (always available, no Docker).
    /// - PostgreSQL: a single Testcontainer, started lazily on first use.
    /// </summary>
    [TestClass]
    public static class TestMain
    {
        public static List<object[]> StoreInstances { get; } = [];

        private static readonly List<string> _sqliteFiles = [];
        private static PostgreSqlContainer _pg;
        private static readonly SemaphoreSlim _pgLock = new(1, 1);

        static TestMain()
        {
            _Setup_Sqlite();
            _Setup_Postgres();
        }

        public static string NewTableName() => ("t" + Guid.NewGuid().ToString("N")).MakeStorageConformName();
        public static string NewStreamId() => "stream-" + Guid.NewGuid().ToString("N");

        private static void _Setup_Sqlite()
        {
            Func<string, Task<IEventStore>> factory = tableName =>
            {
                string file = Path.Combine(Path.GetTempPath(), $"pp_es_{Guid.NewGuid():N}.db");
                lock (_sqliteFiles)
                    _sqliteFiles.Add(file);

                IEventStore store = new Dapper_EventStore("SQLite.MS", $"Data Source={file}", tableName);
                return Task.FromResult(store);
            };
            StoreInstances.Add([factory]);
        }

        private static void _Setup_Postgres()
        {
            Func<string, Task<IEventStore>> factory = async tableName =>
            {
                await _EnsurePostgres().ConfigureAwait(false);
                return new Dapper_EventStore("PostgreSQL", _pg.GetConnectionString(), tableName);
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
