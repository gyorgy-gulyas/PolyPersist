using Cassandra;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PolyPersist.Net.EventStore.Cassandra;
using PolyPersist.Net.EventStore.Dapper;
using PolyPersist.Net.EventStore.Memory;
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
            _Setup_Memory();
            _Setup_Sqlite();
            _Setup_Postgres();
            _Setup_Scylla();
        }

        private static void _Setup_Memory()
        {
            Func<string, Task<IEventStore>> factory = _ => Task.FromResult<IEventStore>(new Memory_EventStore());
            StoreInstances.Add([factory]);
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

        private static IContainer _scylla;
        private static readonly SemaphoreSlim _scyllaLock = new(1, 1);

        private static void _Setup_Scylla()
        {
            Func<string, Task<IEventStore>> factory = async keyspace =>
            {
                await _EnsureScylla().ConfigureAwait(false);

                var host = _scylla.Hostname;
                var port = _scylla.GetMappedPublicPort(9042);

                var cluster = Cluster.Builder().AddContactPoint(host).WithPort(port)
                    .WithCredentials("cassandra", "cassandra").Build();
                using (var session = await cluster.ConnectAsync().ConfigureAwait(false))
                    await session.ExecuteAsync(new SimpleStatement(
                        $"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}")).ConfigureAwait(false);

                return new Cassandra_EventStore($"host={host};port={port};username=cassandra;password=cassandra;keyspace={keyspace}");
            };
            StoreInstances.Add([factory]);
        }

        private static async Task _EnsureScylla()
        {
            if (_scylla != null)
                return;

            await _scyllaLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_scylla != null)
                    return;

                var container = new ContainerBuilder()
                    .WithImage("scylladb/scylla:5.4")
                    .WithCleanUp(true)
                    .WithPortBinding(9042, assignRandomHostPort: true)
                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9042))
                    .Build();

                await container.StartAsync().ConfigureAwait(false);
                _scylla = container;
            }
            finally
            {
                _scyllaLock.Release();
            }
        }

        [AssemblyInitialize]
        public static Task AssemblyInit(TestContext _) => Task.CompletedTask;

        [AssemblyCleanup]
        public static async Task Cleanup()
        {
            if (_pg != null)
                await _pg.DisposeAsync().ConfigureAwait(false);
            if (_scylla != null)
                await _scylla.DisposeAsync().ConfigureAwait(false);

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
