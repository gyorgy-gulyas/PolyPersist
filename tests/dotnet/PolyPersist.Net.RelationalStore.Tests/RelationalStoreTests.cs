namespace PolyPersist.Net.RelationalStore.Tests
{
    [TestClass]
    [DoNotParallelize]
    public class RelationalStoreTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task StorageModel_Is_Relational(Func<string, Task<IRelationalStore>> factory)
        {
            var store = await factory(TestMain.NewTableName());
            Assert.AreEqual(IStore.StorageModels.Relational, ((IStore)store).StorageModel);
            Assert.IsFalse(string.IsNullOrEmpty(((IStore)store).ProviderName));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Table_Lifecycle_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var name = TestMain.NewTableName();
            var store = await factory(name);

            Assert.IsFalse(await store.IsTableExists(name));

            var table = await store.CreateTable<SampleRecord>(name);
            Assert.IsNotNull(table);
            Assert.AreEqual(name, table.Name);
            Assert.AreSame(store, table.ParentStore);
            Assert.IsTrue(await store.IsTableExists(name));

            // GetTableByName returns a usable handle for the same table
            var again = await store.GetTableByName<SampleRecord>(name);
            Assert.AreEqual(name, again.Name);

            await store.DropTable(name);
            Assert.IsFalse(await store.IsTableExists(name));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task CreateTable_Twice_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var name = TestMain.NewTableName();
            var store = await factory(name);
            await store.CreateTable<SampleRecord>(name);

            var ex = await Assert.ThrowsExceptionAsync<Exception>(() => store.CreateTable<SampleRecord>(name));
            Assert.IsTrue(ex.Message.Contains("already exists"));

            await store.DropTable(name);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GetTableByName_Missing_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var store = await factory(TestMain.NewTableName());
            var ex = await Assert.ThrowsExceptionAsync<Exception>(() => store.GetTableByName<SampleRecord>(TestMain.NewTableName()));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task DropTable_Missing_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var store = await factory(TestMain.NewTableName());
            var ex = await Assert.ThrowsExceptionAsync<Exception>(() => store.DropTable(TestMain.NewTableName()));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }
    }
}
