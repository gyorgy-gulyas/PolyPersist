using PolyPersist.Net.Context;
using PolyPersist.Net.Extensions;

namespace PolyPersist.Net.Tests
{
    [TestClass]
    public class StoreContextTests
    {
        private class TestContext : StoreContext
        {
            public TestContext(IStoreProvider provider) : base(provider) { }
        }

        // ---- Document collection ----

        [TestMethod]
        public async Task GetOrCreateDocumentCollection_Exists_UsesGetByName()
        {
            var store = new FakeDocumentStore(exists: true);
            var ctx = new TestContext(new FakeStoreProvider(doc: store));

            var collection = await ctx.GetOrCreateDocumentCollection<FakeDocument>();

            Assert.IsTrue(store.GetByNameCalled);
            Assert.IsFalse(store.CreateCalled);
            Assert.AreEqual(nameof(FakeDocument), collection.Name);
        }

        [TestMethod]
        public async Task GetOrCreateDocumentCollection_NotExists_Creates_WithCustomName()
        {
            var store = new FakeDocumentStore(exists: false);
            var ctx = new TestContext(new FakeStoreProvider(doc: store));

            var collection = await ctx.GetOrCreateDocumentCollection<FakeDocument>("custom");

            Assert.IsTrue(store.CreateCalled);
            Assert.IsFalse(store.GetByNameCalled);
            Assert.AreEqual("custom", collection.Name);
        }

        // ---- Column table ----

        [TestMethod]
        public async Task GetOrCreateColumnTable_Exists()
        {
            var store = new FakeColumnStore(exists: true);
            var ctx = new TestContext(new FakeStoreProvider(col: store));

            var table = await ctx.GetOrCreateColumnTable<FakeRow>();
            Assert.AreEqual(nameof(FakeRow).ToLower(), table.Name);
        }

        [TestMethod]
        public async Task GetOrCreateColumnTable_NotExists_Creates()
        {
            var store = new FakeColumnStore(exists: false);
            var ctx = new TestContext(new FakeStoreProvider(col: store));

            var table = await ctx.GetOrCreateColumnTable<FakeRow>("mytable");
            Assert.AreEqual("mytable", table.Name);
        }

        // ---- Blob container ----

        [TestMethod]
        public async Task GetOrCreateBlobContainer_Exists()
        {
            var store = new FakeBlobStore(exists: true);
            var ctx = new TestContext(new FakeStoreProvider(blob: store));

            var container = await ctx.GetOrCreateBlobContainer<FakeBlob>();
            Assert.AreEqual(nameof(FakeBlob), container.Name);
        }

        [TestMethod]
        public async Task GetOrCreateBlobContainer_NotExists_Creates()
        {
            var store = new FakeBlobStore(exists: false);
            var ctx = new TestContext(new FakeStoreProvider(blob: store));

            var container = await ctx.GetOrCreateBlobContainer<FakeBlob>("bucket");
            Assert.AreEqual("bucket", container.Name);
        }
    }

    [TestClass]
    public class ExtensionsTests
    {
        [TestMethod]
        public void DocumentCollection_AsQueryable_Happy()
        {
            var collection = new FakeDocumentCollection<FakeDocument>("c");
            var q = collection.AsQueryable();
            Assert.IsNotNull(q);
            Assert.AreEqual(0, q.Count());
        }

        [TestMethod]
        public void DocumentCollection_AsQueryable_Null_Throws()
        {
            IDocumentCollection<FakeDocument>? collection = null;
            Assert.ThrowsException<ArgumentNullException>(() => collection!.AsQueryable());
        }

        [TestMethod]
        public void DocumentCollection_AsQueryable_NullQuery_ThrowsInvalidCast()
        {
            var collection = new FakeDocumentCollection<FakeDocument>("c", nullQuery: true);
            Assert.ThrowsException<InvalidCastException>(() => collection.AsQueryable());
        }

        [TestMethod]
        public void DocumentCollection_AsQueryable_OfType_Happy()
        {
            var collection = new FakeDocumentCollection<FakeDocument>("c");
            var q = collection.AsQueryable<FakeDocument, FakeDocument>();
            Assert.IsNotNull(q);
        }

        [TestMethod]
        public void DocumentCollection_AsQueryable_OfType_Null_Throws()
        {
            IDocumentCollection<FakeDocument>? collection = null;
            Assert.ThrowsException<ArgumentNullException>(
                () => collection!.AsQueryable<FakeDocument, FakeDocument>());
        }

        [TestMethod]
        public void ColumnTable_AsQueryable_Happy()
        {
            var table = new FakeColumnTable<FakeRow>("t");
            var q = table.AsQueryable();
            Assert.IsNotNull(q);
            Assert.AreEqual(0, q.Count());
        }

        [TestMethod]
        public void ColumnTable_AsQueryable_Null_Throws()
        {
            IColumnTable<FakeRow>? table = null;
            Assert.ThrowsException<ArgumentNullException>(() => table!.AsQueryable());
        }

        [TestMethod]
        public void ColumnTable_AsQueryable_NullQuery_ThrowsInvalidCast()
        {
            var table = new FakeColumnTable<FakeRow>("t", nullQuery: true);
            Assert.ThrowsException<InvalidCastException>(() => table.AsQueryable());
        }

        [TestMethod]
        public void AsAsync_NotAsyncQueryable_Throws()
        {
            var q = Enumerable.Empty<FakeRow>().AsQueryable();
            Assert.ThrowsException<InvalidCastException>(() => q.AsAsync());
        }
    }
}
