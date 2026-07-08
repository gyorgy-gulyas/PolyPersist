using PolyPersist.Net.Transactions;

namespace PolyPersist.Net.RelationalStore.Tests
{
    /// <summary>
    /// Exercises the new ITransaction ITable overloads (compensation-based cross-store unit of work)
    /// against the relational store.
    /// </summary>
    [TestClass]
    [DoNotParallelize]
    public class RelationalTransactionTests
    {
        private static async Task<ITable<SampleRecord>> NewTable(Func<string, Task<IRelationalStore>> factory)
        {
            var name = TestMain.NewTableName();
            var store = await factory(name);
            return await store.CreateTable<SampleRecord>(name);
        }

        private static SampleRecord Sample(string pk = "p1", string name = "Alice")
            => new() { PartitionKey = pk, Name = name, Age = 30, Balance = 100m };

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_Commit_KeepsRow(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();

            await using (var tx = new Transaction())
            {
                await tx.Insert(table, rec);
                await tx.Commit();
            }

            Assert.IsNotNull(await table.Find(rec.PartitionKey, rec.id));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_Rollback_RemovesRow(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();

            var tx = new Transaction();
            await tx.Insert(table, rec);
            await tx.Rollback();

            Assert.IsNull(await table.Find(rec.PartitionKey, rec.id));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_Dispose_WithoutCommit_RollsBack(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();

            await using (var tx = new Transaction())
            {
                await tx.Insert(table, rec);
                // no commit -> DisposeAsync rolls back
            }

            Assert.IsNull(await table.Find(rec.PartitionKey, rec.id));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Update_Rollback_RestoresOriginal(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample(name: "original");
            await table.Insert(rec);

            var tx = new Transaction();
            tx.AddOriginal(table, rec);
            rec.Name = "modified";
            await tx.Update(table, rec);
            await tx.Rollback();

            var found = await table.Find(rec.PartitionKey, rec.id);
            Assert.AreEqual("original", found.Name);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Update_Commit_KeepsChange(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample(name: "original");
            await table.Insert(rec);

            await using (var tx = new Transaction())
            {
                tx.AddOriginal(table, rec);
                rec.Name = "modified";
                await tx.Update(table, rec);
                await tx.Commit();
            }

            var found = await table.Find(rec.PartitionKey, rec.id);
            Assert.AreEqual("modified", found.Name);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Delete_Rollback_ReinsertsRow(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            await table.Insert(rec);

            var tx = new Transaction();
            tx.AddOriginal(table, rec);
            await tx.Delete(table, rec);
            Assert.IsNull(await table.Find(rec.PartitionKey, rec.id));

            await tx.Rollback();
            Assert.IsNotNull(await table.Find(rec.PartitionKey, rec.id));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Delete_Commit_RemovesRow(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            await table.Insert(rec);

            await using (var tx = new Transaction())
            {
                tx.AddOriginal(table, rec);
                await tx.Delete(table, rec);
                await tx.Commit();
            }

            Assert.IsNull(await table.Find(rec.PartitionKey, rec.id));
        }
    }
}
