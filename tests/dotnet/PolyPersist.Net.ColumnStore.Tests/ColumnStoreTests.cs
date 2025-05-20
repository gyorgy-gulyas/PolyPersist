using PolyPersist.Net.Core;
using PolyPersist.Net.Test;
using System.Reflection;


namespace PolyPersist.Net.ColumnStore.Tests
{
    #region
    class SampleRow : Entity, IRow
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
    public class ColumnStoreTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_BasicInfo_OK(Func<Task<IColumnStore>> factory)
        {
            var store = await factory();

            Assert.IsNotNull(store);
            Assert.AreEqual(IStore.StorageModels.ColumnStore, store.StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(store.ProviderName));
        }


        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_Table_Ok(Func<Task<IColumnStore>> factory)
        {
            var store = await factory();

            Assert.IsFalse(await store.IsTableExists("sampletable"));

            var table = await store.CreateTable<SampleRow>("sampletable");
            Assert.IsTrue(await store.IsTableExists("sampletable"));
            Assert.AreEqual("sampletable", table.Name);
            Assert.IsNotNull(table.GetUnderlyingImplementation());

            var get = await store.GetTableByName<SampleRow>("sampletable");
            Assert.IsNotNull(get);
            Assert.AreEqual("sampletable", get.Name);

            await store.DropTable("sampletable");
            Assert.IsFalse(await store.IsTableExists("sampletable"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_CreateTable_Fail(Func<Task<IColumnStore>> factory)
        {
            var store = await factory();

            var table = await store.CreateTable<SampleRow>("sampletable");

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.CreateTable<SampleRow>("sampletable"));
            Assert.IsTrue(ex.Message.Contains("already exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_GetTable_Fail(Func<Task<IColumnStore>> factory)
        {
            var store = await factory();

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.GetTableByName<SampleRow>("notexist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task BlobStore_DropTable_Fail(Func<Task<IColumnStore>> factory)
        {
            var store = await factory();

            Exception ex = await Assert.ThrowsExceptionAsync<Exception>(async () => await store.DropTable("notexist"));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }
    }
}