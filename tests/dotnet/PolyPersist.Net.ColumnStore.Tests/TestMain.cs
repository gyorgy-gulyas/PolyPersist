using Cassandra;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using PolyPersist.Net.ColumnStore.Cassandra;

namespace PolyPersist.Net.ColumnStore.Tests
{
    [TestClass]
    public class TestMain
    {
        public static List<object[]> StoreInstances { get; } = new();

        static TestMain()
        {
            _Setup_Cassandra_ColumnStore();
        }

        [AssemblyInitialize]
        public static Task InitializeContext(TestContext tc)
        {
            return Task.CompletedTask;
        }

        [ClassCleanup]
        public static async Task Cleanup()
        {
            if (_cassandraContainer != null)
            {
                await _cassandraContainer.StopAsync();
                await _cassandraContainer.DisposeAsync();
                _cassandraContainer = null;
            }
        }

       private static IContainer _cassandraContainer;
        private static readonly SemaphoreSlim _cassandraInitLock = new(1, 1);
        private static void _Setup_Cassandra_ColumnStore()
        {
            var functor = new object[] {
                new Func<string, Task<IColumnStore>>( async (testname) => {
                    if (_cassandraContainer == null)
                    {
                        await _cassandraInitLock.WaitAsync();
                        try
                        {
                            if (_cassandraContainer == null) // re check!
                            {
                                _cassandraContainer = new ContainerBuilder()
                                    .WithImage("cassandra:4.1")
                                    .WithCleanUp(true)
                                    .WithEnvironment("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator") // Authenticator engedélyezése
                                    .WithEnvironment("CASSANDRA_PASSWORD_SEEDER", "true")
                                    .WithEnvironment("CASSANDRA_USERNAME", "cassandra")
                                    .WithEnvironment("CASSANDRA_PASSWORD", "cassandra")
                                    .WithPortBinding(9042, assignRandomHostPort: true) // 9042 CQL port
                                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9042))
                                    .Build();

                                // Start container
                                await _cassandraContainer.StartAsync();
                            }
                        }
                        finally
                        {
                            _cassandraInitLock.Release();
                        }
                    }
                    var hostPort = _cassandraContainer.GetMappedPublicPort(9042);
                    var hostName = _cassandraContainer.Hostname;

                    var cluster = Cluster.Builder()
                        .AddContactPoint(hostName)
                        .WithPort(hostPort)
                        .WithCredentials("cassandra", "cassandra")
                        .Build();

                    using var session = cluster.Connect();
                    session.Execute("CREATE KEYSPACE IF NOT EXISTS testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

                    string connectionString = $"type=minIO;host={hostName};port={hostPort};username=cassandra;password=cassandra;keyspace=testks";
                    return new Cassandra_ColumnStore(connectionString);
                })
            };
            StoreInstances.Add(functor);
        }
    }
}
