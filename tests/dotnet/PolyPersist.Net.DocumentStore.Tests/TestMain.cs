using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MongoDB.Driver;
using PolyPersist.Net.DocumentStore.Memory;
using PolyPersist.Net.DocumentStore.MongoDB;
using Testcontainers.MongoDb;

namespace PolyPersist.Net.DocumentStore.Tests
{
    [TestClass]
    public class TestMain
    {
        public static List<object[]> StoreInstances { get; } = new();

        static TestMain()
        {
            _Setup_Memory_DocumentStore();
            _Setup_Mongo_DocumentStore();
        }

        [AssemblyInitialize]
        public static Task InitializeContext(TestContext tc)
        {
            return Task.CompletedTask;
        }

        [ClassCleanup]
        public static async Task Cleanup()
        {
            if (_mongoContainer != null)
            {
                await _mongoContainer.StopAsync();
                await _mongoContainer.DisposeAsync();
                _mongoContainer = null;
            }
        }

        private static void _Setup_Memory_DocumentStore()
        {
            var functor = new object[] {
                new Func<string, Task<IDocumentStore>>( (testname) => {
                    var store = new Memory_DocumentStore("");
                    return Task.FromResult<IDocumentStore>( store );
                } )
            };
            StoreInstances.Add(functor);
        }

        private static MongoDbContainer _mongoContainer;
        private static readonly SemaphoreSlim _mongoInitLock = new(1, 1);
        private static void _Setup_Mongo_DocumentStore()
        {
            var functor = new object[] {
                new Func<string, Task<IDocumentStore>>( async (testname) => {
                    if (_mongoContainer == null)
                    {
                        await _mongoInitLock.WaitAsync();
                        try
                        {
                            if (_mongoContainer == null) // re check!
                            {
                                // create mongo container
                                _mongoContainer = new MongoDbBuilder()
                                    .WithImage("mongo:latest")
                                    .WithCleanUp(true)
                                    .WithEnvironment("MONGO_INITDB_ROOT_USERNAME", "mongo")
                                    .WithEnvironment("MONGO_INITDB_ROOT_PASSWORD", "mongo")
                                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(27017))
                                    .Build();

                                // Start container
                                await _mongoContainer.StartAsync();
                            }
                        }
                        finally
                        {
                            _mongoInitLock.Release();
                        }
                    }
                    var builder = new MongoUrlBuilder(_mongoContainer.GetConnectionString())
                    {
                        DatabaseName = testname,
                        AuthenticationSource = "admin" // this database is contains the user mongo/mongo 
                    };
                    return new MongoDB_DocumentStore(builder.ToString());
                })
            };
            StoreInstances.Add(functor);
        }
    }
}
