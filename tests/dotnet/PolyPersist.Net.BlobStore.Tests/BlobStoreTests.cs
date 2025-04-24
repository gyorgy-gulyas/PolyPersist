using PolyPersist.Net.Test;
using System.Reflection;
using Blob = PolyPersist.Net.Core.Blob;


namespace PolyPersist.Net.BlobStore.Tests
{
    #region
    class SampleBlob : Blob
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
    public class BlobStoreTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_BasicInfo_OK(Func<string,Task<IBlobStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            Assert.IsNotNull(store);
            Assert.AreEqual(IStore.StorageModels.BlobStore, store.StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(store.ProviderName));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Container_Ok(Func<string, Task<IBlobStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            Assert.IsFalse(await store.IsContainerExists("container"));

            var container = await store.CreateContainer<SampleBlob>("container");
            Assert.IsTrue(await store.IsContainerExists("container"));
            Assert.AreEqual("container", container.Name);
            Assert.IsNotNull(container.GetUnderlyingImplementation());

            var get = await store.GetContainerByName<SampleBlob>("container");
            Assert.IsNotNull(get);
            Assert.AreEqual("container", get.Name);

            await store.DropContainer("container");
            Assert.IsFalse(await store.IsContainerExists("container"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_CreateContainer_Fail(Func<string, Task<IBlobStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            var container = await store.CreateContainer<SampleBlob>("container");

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>( async () => await store.CreateContainer<SampleBlob>("container") );
            Assert.IsTrue(ex.Message.Contains("already exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_GetContainer_Fail(Func<string, Task<IBlobStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.GetContainerByName<SampleBlob>("not_exist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_DropContainer_Fail(Func<string, Task<IBlobStore>> factory)
        {
            var store = await factory(MethodBase.GetCurrentMethod().GetAsyncMethodName());

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.DropContainer("not_exist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }
    }
}