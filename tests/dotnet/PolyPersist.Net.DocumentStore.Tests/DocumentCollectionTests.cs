using PolyPersist.Net.Extensions;
using PolyPersist.Net.Test;
using System;
using System.Reflection;

namespace PolyPersist.Net.DocumentStore.Tests
{
    [TestClass]
    public class DocumentCollectionTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_GetUnderlyingImplementation_OK(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var collection = await store.CreateCollection<SampleDocument>("sampletable");

            Assert.IsNotNull(collection.Name);
            Assert.IsNotNull(collection.GetUnderlyingImplementation());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Insert_And_Find_OK(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var doc = new SampleDocument { PartitionKey = "p1", id = "d1", str_value = "abc" };
            await col.Insert(doc);

            var loaded = await col.Find("p1", "d1");
            Assert.IsNotNull(loaded);
            Assert.AreEqual("abc", loaded.str_value);
            Assert.AreEqual("d1", loaded.id);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Insert_Duplicate_Fails(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var doc1 = new SampleDocument { PartitionKey = "p1", id = "d1" };
            var doc2 = new SampleDocument { PartitionKey = "p1", id = "d1" };
            await col.Insert(doc1);

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () => await col.Insert(doc2));
            Assert.IsTrue(exception.Message.Contains("duplicate key"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Delete_RemovesDocument(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var doc = new SampleDocument { PartitionKey = "p1", id = "d1" };
            await col.Insert(doc);
            await col.Delete("p1", "d1");

            var found = await col.Find("p1", "d1");
            Assert.IsNull(found);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Update_ChangesEtag(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var doc = new SampleDocument { PartitionKey = "p1", id = "d1", str_value = "before" };
            await col.Insert(doc);
            var original = await col.Find("p1", "d1");

            await Task.Delay(5);
            doc.str_value = "after";
            await col.Update(doc);

            var updated = await col.Find("p1", "d1");
            Assert.AreNotEqual(original.etag, updated.etag);
            Assert.IsTrue(updated.LastUpdate > original.LastUpdate);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Insert_Etag_And_LastUpdate_Set(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var doc = new SampleDocument { PartitionKey = "p1", id = "d1" };
            await col.Insert(doc);

            var found = await col.Find("p1", "d1");
            Assert.IsFalse(string.IsNullOrEmpty(found.etag));
            Assert.IsTrue(found.LastUpdate > DateTime.MinValue);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Update_NotFound_Fails(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var missing = new SampleDocument { PartitionKey = "p1", id = "notfound", etag = "fake-etag" };

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await col.Update(missing);
            });
            Assert.IsTrue(exception.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Delete_NotFound_Fails(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await col.Delete("pk-missing", "id-missing");
            });
            Assert.IsTrue(exception.Message.Contains("already removed"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Find_NotFound_ReturnsNull(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var result = await col.Find("nonexistent", "nope");
            Assert.IsNull(result);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Insert_DistinctEtagsForDistinctDocuments(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var d1 = new SampleDocument { PartitionKey = "pk", id = "d1" };
            var d2 = new SampleDocument { PartitionKey = "pk", id = "d2" };

            await col.Insert(d1);
            await col.Insert(d2);

            var found1 = await col.Find("pk", "d1");
            var found2 = await col.Find("pk", "d2");

            Assert.AreNotEqual(found1.etag, found2.etag);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Document_Query_SimpleFilter_OK(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());
            var col = await store.CreateCollection<SampleDocument>("docs");

            var items = new[]
            {
                new SampleDocument { PartitionKey = "pk1", id = "i1", int_value = 10 },
                new SampleDocument { PartitionKey = "pk1", id = "i2", int_value = 20 },
                new SampleDocument { PartitionKey = "pk2", id = "i3", int_value = 30 },
            };

            foreach (var doc in items)
                await col.Insert(doc);

            var list = col.AsQueryable().Where(d => d.PartitionKey == "pk1").ToList();
            Assert.AreEqual(2, list.Count);

            list = col.AsQueryable().Where(d => d.int_value > 10 && d.int_value < 30).ToList();
            Assert.AreEqual(1, list.Count);
            Assert.AreEqual("i2", list[0].id);
        }
    }
}