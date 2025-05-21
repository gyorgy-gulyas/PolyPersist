using PolyPersist.Net.Test;
using System.Reflection;

namespace PolyPersist.Net.BlobStore.Tests
{
    [TestClass]
    public class BlobStoreTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_BasicInfo_OK(Func<string, Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            Assert.IsNotNull(store);
            Assert.AreEqual(IStore.StorageModels.BlobStore, store.StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(store.ProviderName));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Container_Ok(Func<string, Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

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
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            var container = await store.CreateContainer<SampleBlob>("container");

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>( async () => await store.CreateContainer<SampleBlob>("container") );
            Assert.IsTrue(ex.Message.Contains("already exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_GetContainer_Fail(Func<string, Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.GetContainerByName<SampleBlob>("notexist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_DropContainer_Fail(Func<string, Task<IBlobStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.DropContainer("notexist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }
    }
}