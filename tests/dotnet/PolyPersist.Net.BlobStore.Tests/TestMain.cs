using Testcontainers.MongoDb;
using PolyPersist.Net.BlobStore.FileSystem;
using PolyPersist.Net.BlobStore.GridFS;
using PolyPersist.Net.BlobStore.Memory;
using DotNet.Testcontainers.Builders;
using MongoDB.Driver;

namespace PolyPersist.Net.BlobStore.Tests
{
    [TestClass]
    public class TestMain
    {
        public static List<object[]> StoreInstances { get; } = new();

        static TestMain()
        {
            _Setup_Memory_BlobStore();
            _Setup_FileSystem_BlobStore();
            _Setup_GridFS_BlobStore();
        }

        [AssemblyInitialize]
        public static Task InitializeContext(TestContext tc)
        {
            return Task.CompletedTask;
        }

        private static void _Setup_Memory_BlobStore()
        {
            var functor = new object[] {
                new Func<string, Task<IBlobStore>>( (testname) => {
                    var store = new Memory_BlobStore("");
                    return Task.FromResult<IBlobStore>( store );
                } )
            };
            StoreInstances.Add(functor);
        }

        private static void _Setup_FileSystem_BlobStore()
        {
            var functor = new object[] {
                new Func<string, Task<IBlobStore>>( (testname) => {
                    var store = new FileSystem_BlobStore(Path.Combine(Path.GetTempPath(), testname, Guid.NewGuid().ToString()));
                    return Task.FromResult<IBlobStore>( store);
                } )
            };
            StoreInstances.Add(functor);
        }

        private static MongoDbContainer _mongoContainer;
        private static readonly SemaphoreSlim _mongoInitLock = new(1, 1);

        private static void _Setup_GridFS_BlobStore()
        {
            var functor = new object[] {
                new Func<string, Task<IBlobStore>>( async (testname) => {
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

                                // Start database
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
                    return new GridFS_BlobStore(builder.ToString());
                })
            };
            StoreInstances.Add(functor);
        }
    }
}
