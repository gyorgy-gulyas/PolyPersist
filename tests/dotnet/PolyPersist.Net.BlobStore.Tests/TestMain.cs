using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MongoDB.Driver;
using PolyPersist.Net.BlobStore.AmazonS3;
using PolyPersist.Net.BlobStore.FileSystem;
using PolyPersist.Net.BlobStore.GoogleCloudStorage;
using PolyPersist.Net.BlobStore.GridFS;
using PolyPersist.Net.BlobStore.Memory;
using PolyPersist.Net.BlobStore.MinIO;
using PolyPersist.Net.BlobStore.AzureStorage;
using Testcontainers.MongoDb;

namespace PolyPersist.Net.BlobStore.Tests
{
    [TestClass]
    public class TestMain
    {
        public static List<object[]> StoreInstances { get; } = new();

        static TestMain()
        {
            //_Setup_Memory_BlobStore();
            //_Setup_FileSystem_BlobStore();
            //_Setup_GridFS_BlobStore();
            //_Setup_MinIO_BlobStore();
            //_Setup_AmazonS3_BlobStore();
            //_Setup_GoogleCloudStorage_BlobStore();
            _Setup_Memory_AzureStorage();
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

            if (_minIOContainer != null)
            {
                await _minIOContainer.StopAsync();
                await _minIOContainer.DisposeAsync();
                _minIOContainer = null;
            }
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
                    return new GridFS_BlobStore(builder.ToString());
                })
            };
            StoreInstances.Add(functor);
        }

        public static IContainer _minIOContainer;
        private static readonly SemaphoreSlim _minIOInitLock = new(1, 1);
        private static void _Setup_MinIO_BlobStore()
        {
            var functor = new object[] {
                new Func<string, Task<IBlobStore>>( async (testname) => {
                    if (_minIOContainer == null)
                    {
                        await _minIOInitLock.WaitAsync();
                        try
                        {
                            if (_minIOContainer == null) // re check!
                            {
                                // create minio container
                                _minIOContainer = new ContainerBuilder()
                                    .WithImage("minio/minio:latest")
                                    .WithCleanUp(true)
                                    .WithPortBinding(9000, true) // bind random host port
                                    .WithEnvironment("MINIO_ROOT_USER", "minioadmin")
                                    .WithEnvironment("MINIO_ROOT_PASSWORD", "minioadmin")
                                    .WithCommand("server", "/data") // required for minio to start properly
                                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
                                    .Build();

                                // Start container
                                await _minIOContainer.StartAsync();
                            }
                        }
                        finally
                        {
                            _minIOInitLock.Release();
                        }
                    }
                    var hostPort = _minIOContainer.GetMappedPublicPort(9000);

                    string connectionString = $"type=minIO;endpoint=localhost;port={hostPort};access-key=minioadmin;secret-key=minioadmin;withssl=false";
                    return new MinIO_BlobStore(connectionString);
                })
            };
            StoreInstances.Add(functor);
        }

        public static IContainer _amazonS3Container;
        private static readonly SemaphoreSlim _amazonS3InitLock = new(1, 1);
        private static void _Setup_AmazonS3_BlobStore()
        {
            var functor = new object[] {
                new Func<string, Task<IBlobStore>>( async (testname) => {
                    if (_amazonS3Container == null)
                    {
                        await _amazonS3InitLock.WaitAsync();
                        try
                        {
                            if (_amazonS3Container == null) // re check!
                            {
                                // create amazon s3 container
                                _amazonS3Container = new ContainerBuilder()
                                    .WithImage("localstack/localstack:latest")
                                    .WithCleanUp(true)
                                    .WithPortBinding(4566, true)
                                    .WithEnvironment("SERVICES", "s3")
                                    .WithEnvironment("AWS_ACCESS_KEY_ID", "amazons3")
                                    .WithEnvironment("AWS_SECRET_ACCESS_KEY", "amazons3")
                                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(4566))
                                    .Build();

                                // Start container
                                await _amazonS3Container.StartAsync();
                            }
                        }
                        finally
                        {
                            _amazonS3InitLock.Release();
                        }
                    }
                    var hostPort = _amazonS3Container.GetMappedPublicPort(4566);;

                    string connectionString = $"type=AmazonS3;endpoint=http://localhost:{hostPort};access-key=amazons3;secret-key=amazons3;withssl=false";
                    return new AmazonS3_BlobStore(connectionString);
                })
            };
            StoreInstances.Add(functor);
        }


        public static IContainer _gcsContainer;
        private static readonly SemaphoreSlim _gcsInitLock = new(1, 1);
        private static void _Setup_GoogleCloudStorage_BlobStore()
        {
            var functor = new object[] {
                new Func<string, Task<IBlobStore>>( async (testname) => {
                    if (_gcsContainer == null)
                    {
                        await _gcsInitLock.WaitAsync();
                        try
                        {
                            if (_gcsContainer == null) // re check!
                            {
                                // create amazon s3 container
                                _gcsContainer = new ContainerBuilder()
                                    .WithImage("fsouza/fake-gcs-server:latest")
                                    .WithCleanUp(true)
                                    .WithPortBinding(4443, true) // https port
                                    .WithCommand("-scheme", "http", "-port", "4443") // https helyett http
                                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(4443))
                                    .Build();

                                // Start container
                                await _gcsContainer.StartAsync();
                            }
                        }
                        finally
                        {
                            _gcsInitLock.Release();
                        }
                    }
                    var hostPort = _gcsContainer.GetMappedPublicPort(4443);

                    string connectionString = $"type=googlecloud;projectid=test-project;baseurl=http://localhost:{hostPort};usetoken=fake";
                    return new GoogleCloudStorage_BlobStore(connectionString);
                })
            };
            StoreInstances.Add(functor);
        }


        public static IContainer _azureitContainer;
        private static readonly SemaphoreSlim _azureitInitLock = new(1, 1);
        private static void _Setup_Memory_AzureStorage()
        {
            var functor = new object[] {
                new Func<string, Task<IBlobStore>>( async (testname) => {
                    if (_azureitContainer == null)
                    {
                        await _azureitInitLock.WaitAsync();
                        try
                        {
                            if (_azureitContainer == null) // re check!
                            {
                                // create azureit container
                                _azureitContainer = new ContainerBuilder()
                                    .WithImage("mcr.microsoft.com/azure-storage/azurite")
                                    .WithCleanUp(true)
                                    .WithPortBinding(10000, assignRandomHostPort: true)
                                    .WithEntrypoint("azurite-blob")
                                    .WithCommand("--blobHost", "0.0.0.0", "--loose")
                                    .WithEnvironment("AZURITE_ACCOUNTS", "devstoreaccount1:Eby8vdM02xNoGVZ4fT8nH0HbC/mFBFTuT7Tt6VtP/Nw=")
                                    .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(10000))
                                    .Build();

                                // Start container
                                await _azureitContainer.StartAsync();
                            }
                        }
                        finally
                        {
                            _azureitInitLock.Release();
                        }
                    }
                    var hostPort = _azureitContainer.GetMappedPublicPort(10000);

                    string connectionString = $"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNoGVZ4fT8nH0HbC/mFBFTuT7Tt6VtP/Nw=;BlobEndpoint=http://127.0.0.1:{hostPort}/devstoreaccount1;";
                    return new AzureStorage_BlobStore(connectionString);
                })
            };
            StoreInstances.Add(functor);
        }
    }
}
