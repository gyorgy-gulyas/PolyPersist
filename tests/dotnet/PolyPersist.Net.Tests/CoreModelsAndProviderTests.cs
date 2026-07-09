using PolyPersist.Net.Core;

namespace PolyPersist.Net.Tests
{
    [TestClass]
    public class CoreModelsTests
    {
        [TestMethod]
        public void Entity_Properties()
        {
            var now = DateTime.UtcNow;
            var e = new Entity { id = "1", etag = "e", PartitionKey = "pk", LastUpdate = now };
            Assert.AreEqual("1", e.id);
            Assert.AreEqual("e", e.etag);
            Assert.AreEqual("pk", e.PartitionKey);
            Assert.AreEqual(now, e.LastUpdate);
        }

        [TestMethod]
        public void Blob_Properties_InheritEntity()
        {
            var b = new Blob { id = "1", etag = "e", PartitionKey = "pk", fileName = "f.txt", contentType = "text/plain" };
            Assert.AreEqual("f.txt", b.fileName);
            Assert.AreEqual("text/plain", b.contentType);
            Assert.AreEqual("1", b.id);
            Assert.IsInstanceOfType(b, typeof(Entity));
            Assert.IsInstanceOfType(b, typeof(IBlob));
        }

        [TestMethod]
        public void Event_Properties()
        {
            var ts = DateTime.UtcNow;
            var ev = new Event
            {
                eventId = "ev1",
                streamId = "s1",
                version = 3,
                eventType = "OrderPlaced",
                data = "{}",
                metadata = "{}",
                timestamp = ts,
            };
            Assert.AreEqual("ev1", ev.eventId);
            Assert.AreEqual("s1", ev.streamId);
            Assert.AreEqual(3, ev.version);
            Assert.AreEqual("OrderPlaced", ev.eventType);
            Assert.AreEqual("{}", ev.data);
            Assert.AreEqual("{}", ev.metadata);
            Assert.AreEqual(ts, ev.timestamp);
            Assert.IsInstanceOfType(ev, typeof(IEvent));
        }
    }

    [TestClass]
    public class StoreProviderTests
    {
        private class ThrowingProvider : StoreProvider { }

        private class WorkingProvider : StoreProvider
        {
            protected override IStore GetRelationalStore() => new FakeStore { StorageModel = IStore.StorageModels.Relational };
            protected override IDocumentStore GetDocumentStore() => new FakeDocumentStore(false);
            protected override IColumnStore GetColumnStore() => new FakeColumnStore(false);
            protected override IBlobStore GetBlobStore() => new FakeBlobStore(false);
        }

        [TestMethod]
        public void GetStore_ReturnsEachStore()
        {
            IStoreProvider provider = new WorkingProvider();
            Assert.AreEqual(IStore.StorageModels.Relational, provider.getStore(IStore.StorageModels.Relational).StorageModel);
            Assert.AreEqual(IStore.StorageModels.Document, provider.getStore(IStore.StorageModels.Document).StorageModel);
            Assert.AreEqual(IStore.StorageModels.ColumnStore, provider.getStore(IStore.StorageModels.ColumnStore).StorageModel);
            Assert.AreEqual(IStore.StorageModels.BlobStore, provider.getStore(IStore.StorageModels.BlobStore).StorageModel);
        }

        [TestMethod]
        public void GetStore_UnhandledModel_Throws()
        {
            IStoreProvider provider = new WorkingProvider();
            Assert.ThrowsException<NotImplementedException>(
                () => provider.getStore(IStore.StorageModels.EventStore));
        }

        [TestMethod]
        public void GetStore_DefaultBaseImplementations_Throw()
        {
            IStoreProvider provider = new ThrowingProvider();
            Assert.ThrowsException<NotImplementedException>(() => provider.getStore(IStore.StorageModels.Relational));
            Assert.ThrowsException<NotImplementedException>(() => provider.getStore(IStore.StorageModels.Document));
            Assert.ThrowsException<NotImplementedException>(() => provider.getStore(IStore.StorageModels.ColumnStore));
            Assert.ThrowsException<NotImplementedException>(() => provider.getStore(IStore.StorageModels.BlobStore));
        }
    }
}
