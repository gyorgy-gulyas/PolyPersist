using PolyPersist.Net.Core;
using PolyPersist.Net.Test;
using System.Reflection;

namespace PolyPersist.Net.DocumentStore.Tests
{
    #region
    class SampleDocument : Document
    {
        public string str_value { get; set; }
        public int int_value { get; set; }
        public decimal decimal_value { get; set; }
        public bool bool_value { get; set; }
        public DateOnly date_value { get; set; }
        public TimeOnly time_value { get; set; }
        public DateTime datetime_value { get; set; }
    }
    #endregion

    [TestClass]
    public class DocumentStoreTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_BasicInfo_OK(Func<string,Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            Assert.IsNotNull(store);
            Assert.AreEqual(IStore.StorageModels.Document, store.StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(store.ProviderName));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DocumentStore_Collection_OK(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            Assert.IsFalse(await store.IsCollectionExists("docs"));
            var col = await store.CreateCollection<SampleDocument>("docs");
            Assert.IsTrue(await store.IsCollectionExists("docs"));
            Assert.AreEqual("docs", col.Name);

            var get = await store.GetCollectionByName<SampleDocument>("docs");
            Assert.IsNotNull(get);
            Assert.AreEqual("docs", get.Name);

            await store.DropCollection("docs");
            Assert.IsFalse(await store.IsCollectionExists("docs"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DocumentStore_CreateCollection_Fail(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            var col = await store.CreateCollection<SampleDocument>("docs");

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.CreateCollection<SampleDocument>("docs"));
            Assert.IsTrue(exception.Message.Contains("already exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DocumentStore_GetCollection_Fail(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.GetCollectionByName<SampleDocument>("notexist"));
            Assert.IsTrue(exception.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DocumentStore_DropCollection_Fail(Func<string, Task<IDocumentStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.DropCollection("notexist"));
            Assert.IsTrue(exception.Message.Contains("does not exist"));
        }
    }
}