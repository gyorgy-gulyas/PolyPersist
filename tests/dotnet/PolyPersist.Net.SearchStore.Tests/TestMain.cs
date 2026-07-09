using System.Diagnostics.CodeAnalysis;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.PostgreSql;
using PolyPersist.Net.SearchStore.Memory;
using PolyPersist.Net.SearchStore.Postgres;
using PolyPersist.Net.SearchStore.OpenSearch;

namespace PolyPersist.Net.SearchStore.Tests
{
    /// <summary>A search document exercising the portable surface: an id plus free-text fields.</summary>
    [ExcludeFromCodeCoverage]
    public class Article : ISearchDocument
    {
        public string id { get; set; } = null!;
        public string Title { get; set; } = null!;
        public string Body { get; set; } = null!;
    }

    /// <summary>
    /// Registers every search backend the contract tests run against. Each entry is a factory that
    /// returns a fresh <see cref="ISearchStore"/>. Memory is always available (Docker-free); the
    /// container-backed backends (PostgreSQL FTS, OpenSearch) are added lazily and disposed on cleanup.
    /// </summary>
    [TestClass]
    public static class TestMain
    {
        // Each entry: [ Func<Task<ISearchStore>> ].
        public static List<object[]> StoreInstances { get; } = [];

        static TestMain()
        {
            _Setup_Memory();
            _Setup_Postgres();
            _Setup_OpenSearch();
        }

        // Globally-unique, storage-conform index name (lower-case hex, starts with a letter):
        // valid as a PostgreSQL table name and an OpenSearch index name.
        public static string NewIndexName() => "idx" + Guid.NewGuid().ToString("N");

        private static void _Setup_Memory()
        {
            Func<Task<ISearchStore>> factory = () => Task.FromResult<ISearchStore>(new Memory_SearchStore(""));
            StoreInstances.Add([factory]);
        }

        private static PostgreSqlContainer _pg = null!;
        private static readonly SemaphoreSlim _pgLock = new(1, 1);

        private static void _Setup_Postgres()
        {
            Func<Task<ISearchStore>> factory = async () =>
            {
                await _EnsurePostgres().ConfigureAwait(false);
                return new Postgres_SearchStore(_pg.GetConnectionString());
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

        private static IContainer _os = null!;
        private static readonly SemaphoreSlim _osLock = new(1, 1);

        private static void _Setup_OpenSearch()
        {
            Func<Task<ISearchStore>> factory = async () =>
            {
                await _EnsureOpenSearch().ConfigureAwait(false);
                return new OpenSearch_SearchStore($"http://localhost:{_os.GetMappedPublicPort(9200)}");
            };
            StoreInstances.Add([factory]);
        }

        private static async Task _EnsureOpenSearch()
        {
            if (_os != null)
                return;

            await _osLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_os != null)
                    return;

                // Single-node, security plugin off (plain HTTP, no auth) for tests.
                var container = new ContainerBuilder()
                    .WithImage("opensearchproject/opensearch:2.11.1")
                    .WithCleanUp(true)
                    .WithPortBinding(9200, true)
                    .WithEnvironment("discovery.type", "single-node")
                    .WithEnvironment("DISABLE_SECURITY_PLUGIN", "true")
                    .WithEnvironment("DISABLE_INSTALL_DEMO_CONFIG", "true")
                    .WithEnvironment("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                    .WithEnvironment("bootstrap.memory_lock", "false")
                    .WithWaitStrategy(Wait.ForUnixContainer()
                        .UntilHttpRequestIsSucceeded(r => r.ForPort(9200).ForPath("/_cluster/health")))
                    .Build();

                await container.StartAsync().ConfigureAwait(false);
                _os = container;
            }
            finally
            {
                _osLock.Release();
            }
        }

        [AssemblyInitialize]
        public static Task AssemblyInit(TestContext _) => Task.CompletedTask;

        [AssemblyCleanup]
        public static async Task Cleanup()
        {
            if (_pg != null)
                await _pg.DisposeAsync().ConfigureAwait(false);
            if (_os != null)
                await _os.DisposeAsync().ConfigureAwait(false);
        }
    }
}
