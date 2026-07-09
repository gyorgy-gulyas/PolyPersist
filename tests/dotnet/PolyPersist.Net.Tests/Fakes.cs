using System.Linq;
using PolyPersist;

namespace PolyPersist.Net.Tests
{
    // ---- Simple model POCOs used across tests ----

    public enum Color { Red, Green, Blue }

    public class FakeDocument : IDocument
    {
        public string id { get; set; } = null!;
        public string etag { get; set; } = null!;
        public string PartitionKey { get; set; } = null!;
        public DateTime LastUpdate { get; set; }
    }

    public class FakeRow : IRow
    {
        public string id { get; set; } = null!;
        public string etag { get; set; } = null!;
        public string PartitionKey { get; set; } = null!;
        public DateTime LastUpdate { get; set; }
    }

    public class FakeBlob : IBlob
    {
        public string id { get; set; } = null!;
        public string etag { get; set; } = null!;
        public string PartitionKey { get; set; } = null!;
        public DateTime LastUpdate { get; set; }
        public string fileName { get; set; } = null!;
        public string contentType { get; set; } = null!;
    }

    // An entity that also validates.
    public class ValidableEntity : IEntity, IValidable
    {
        public string id { get; set; } = null!;
        public string etag { get; set; } = null!;
        public string PartitionKey { get; set; } = null!;
        public DateTime LastUpdate { get; set; }

        public bool IsValid { get; set; } = true;
        public string ErrorText { get; set; } = "invalid";

        public bool Validate(IList<IValidationError> errors)
        {
            if (IsValid)
                return true;

            errors.Add(new PolyPersist.Net.Common.ValidationError
            {
                TypeOfEntity = nameof(ValidableEntity),
                MemberOfEntity = nameof(id),
                ErrorText = ErrorText,
            });
            return false;
        }
    }

    // A plain entity (no IValidable).
    public class PlainEntity : IEntity
    {
        public string id { get; set; } = null!;
        public string etag { get; set; } = null!;
        public string PartitionKey { get; set; } = null!;
        public DateTime LastUpdate { get; set; }
    }

    // ---- Fake stores for StoreContext / StoreProvider tests ----

    public class FakeStore : IStore
    {
        public IStore.StorageModels StorageModel { get; init; }
        public string ProviderName => "Fake";
    }

    public class FakeDocumentCollection<TDocument> : IDocumentCollection<TDocument>
        where TDocument : IDocument, new()
    {
        private readonly bool _nullQuery;
        public FakeDocumentCollection(string name, bool nullQuery = false) { Name = name; _nullQuery = nullQuery; }
        public IStore ParentStore { get; } = new FakeStore { StorageModel = IStore.StorageModels.Document };
        public string Name { get; }
        public Task Insert(TDocument document) => Task.CompletedTask;
        public Task Update(TDocument document) => Task.CompletedTask;
        public Task Delete(string partitionKey, string id) => Task.CompletedTask;
        public Task<TDocument> Find(string partitionKey, string id) => Task.FromResult(default(TDocument)!);
        public IQueryable<TDocument> Query() => _nullQuery ? null! : Enumerable.Empty<TDocument>().AsQueryable();
        public object GetUnderlyingImplementation() => this;
    }

    public class FakeDocumentStore : IDocumentStore
    {
        private readonly bool _exists;
        public bool GetByNameCalled;
        public bool CreateCalled;
        public FakeDocumentStore(bool exists) { _exists = exists; }
        public IStore.StorageModels StorageModel => IStore.StorageModels.Document;
        public string ProviderName => "Fake";
        public Task<bool> IsCollectionExists(string collectionName) => Task.FromResult(_exists);
        public Task<IDocumentCollection<TDocument>> GetCollectionByName<TDocument>(string collectionName) where TDocument : IDocument, new()
        {
            GetByNameCalled = true;
            return Task.FromResult<IDocumentCollection<TDocument>>(new FakeDocumentCollection<TDocument>(collectionName));
        }
        public Task<IDocumentCollection<TDocument>> CreateCollection<TDocument>(string collectionName) where TDocument : IDocument, new()
        {
            CreateCalled = true;
            return Task.FromResult<IDocumentCollection<TDocument>>(new FakeDocumentCollection<TDocument>(collectionName));
        }
        public Task DropCollection(string collectionName) => Task.CompletedTask;
    }

    public class FakeColumnTable<TRow> : IColumnTable<TRow>
        where TRow : IRow, new()
    {
        private readonly bool _nullQuery;
        public FakeColumnTable(string name, bool nullQuery = false) { Name = name; _nullQuery = nullQuery; }
        public IStore ParentStore { get; } = new FakeStore { StorageModel = IStore.StorageModels.ColumnStore };
        public string Name { get; }
        public Task Insert(TRow row) => Task.CompletedTask;
        public Task Update(TRow row) => Task.CompletedTask;
        public Task Delete(string partitionKey, string id) => Task.CompletedTask;
        public Task<TRow> Find(string partitionKey, string id) => Task.FromResult(default(TRow)!);
        public IQueryable<TRow> Query() => _nullQuery ? null! : Enumerable.Empty<TRow>().AsQueryable();
        public object GetUnderlyingImplementation() => this;
    }

    public class FakeColumnStore : IColumnStore
    {
        private readonly bool _exists;
        public FakeColumnStore(bool exists) { _exists = exists; }
        public IStore.StorageModels StorageModel => IStore.StorageModels.ColumnStore;
        public string ProviderName => "Fake";
        public Task<bool> IsTableExists(string tableName) => Task.FromResult(_exists);
        public Task<IColumnTable<TRow>> GetTableByName<TRow>(string tableName) where TRow : IRow, new()
            => Task.FromResult<IColumnTable<TRow>>(new FakeColumnTable<TRow>(tableName));
        public Task<IColumnTable<TRow>> CreateTable<TRow>(string tableName) where TRow : IRow, new()
            => Task.FromResult<IColumnTable<TRow>>(new FakeColumnTable<TRow>(tableName));
        public Task DropTable(string tableName) => Task.CompletedTask;
    }

    public class FakeBlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        public FakeBlobContainer(string name) { Name = name; }
        public IStore ParentStore { get; } = new FakeStore { StorageModel = IStore.StorageModels.BlobStore };
        public string Name { get; }
        public Task Upload(TBlob blob, Stream content) => Task.CompletedTask;
        public Task<Stream> Download(TBlob blob) => Task.FromResult<Stream>(new MemoryStream());
        public Task Delete(string partitionKey, string id) => Task.CompletedTask;
        public Task<TBlob> Find(string partitionKey, string id) => Task.FromResult(default(TBlob)!);
        public Task UpdateContent(TBlob blob, Stream content) => Task.CompletedTask;
        public Task UpdateMetadata(TBlob blob) => Task.CompletedTask;
        public object GetUnderlyingImplementation() => this;
    }

    public class FakeBlobStore : IBlobStore
    {
        private readonly bool _exists;
        public FakeBlobStore(bool exists) { _exists = exists; }
        public IStore.StorageModels StorageModel => IStore.StorageModels.BlobStore;
        public string ProviderName => "Fake";
        public Task<bool> IsContainerExists(string containerName) => Task.FromResult(_exists);
        public Task<IBlobContainer<TBlob>> GetContainerByName<TBlob>(string containerName) where TBlob : IBlob, new()
            => Task.FromResult<IBlobContainer<TBlob>>(new FakeBlobContainer<TBlob>(containerName));
        public Task<IBlobContainer<TBlob>> CreateContainer<TBlob>(string containerName) where TBlob : IBlob, new()
            => Task.FromResult<IBlobContainer<TBlob>>(new FakeBlobContainer<TBlob>(containerName));
        public Task DropContainer(string containerName) => Task.CompletedTask;
    }

    public class FakeStoreProvider : IStoreProvider
    {
        private readonly IStore? _doc;
        private readonly IStore? _col;
        private readonly IStore? _blob;
        public FakeStoreProvider(IStore? doc = null, IStore? col = null, IStore? blob = null)
        {
            _doc = doc; _col = col; _blob = blob;
        }
        public IStore getStore(IStore.StorageModels storageModel) => storageModel switch
        {
            IStore.StorageModels.Document => _doc!,
            IStore.StorageModels.ColumnStore => _col!,
            IStore.StorageModels.BlobStore => _blob!,
            _ => throw new NotImplementedException(),
        };
    }
}
