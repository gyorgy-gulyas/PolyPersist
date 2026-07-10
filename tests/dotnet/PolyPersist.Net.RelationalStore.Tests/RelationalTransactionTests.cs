using PolyPersist.Net.Common;
using PolyPersist.Net.Core;
using PolyPersist.Net.DocumentStore.Memory;
using PolyPersist.Net.Transactions;

namespace PolyPersist.Net.RelationalStore.Tests
{
    public class TxDoc : Entity, IDocument
    {
        public string str_value { get; set; } = null!;
    }

    /// <summary>
    /// Exercises ITransaction against the relational store: the writes are deferred to Commit(), where
    /// they run inside one native database transaction covering every table of the store.
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
            // deferred: the delete is queued, the row is still there
            Assert.IsNotNull(await table.Find(rec.PartitionKey, rec.id));

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

        // Nothing is written before Commit(), so an uncommitted insert is invisible to every reader -
        // there is no window in which a half-finished unit of work can be observed.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_IsNotVisible_BeforeCommit(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();

            var tx = new Transaction();
            await tx.Insert(table, rec);

            Assert.IsFalse(string.IsNullOrEmpty(rec.id), "the caller must get the id straight away");
            Assert.IsNull(await table.Find(rec.PartitionKey, rec.id));

            await tx.Commit();
            Assert.IsNotNull(await table.Find(rec.PartitionKey, rec.id));
        }

        // The whole point of the native transaction: two tables of the same store commit together.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task MultiTable_Commit_WritesBothTables(Func<string, Task<IRelationalStore>> factory)
        {
            var (_, customers, orders) = await NewTwoTables(factory);
            var customer = new Customer { PartitionKey = "p1", Name = "Alice" };
            var order = new Order { PartitionKey = "p1", CustomerId = "c1", Total = 42m };

            await using (var tx = new Transaction())
            {
                await tx.Insert(customers, customer);
                await tx.Insert(orders, order);
                await tx.Commit();
            }

            Assert.IsNotNull(await customers.Find("p1", customer.id));
            Assert.IsNotNull(await orders.Find("p1", order.id));
        }

        // ... and a failure anywhere in the unit of work leaves NEITHER table changed: the native
        // transaction is rolled back, not compensated row by row.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task MultiTable_WhenOneOperationFails_NeitherTableIsWritten(Func<string, Task<IRelationalStore>> factory)
        {
            var (_, customers, orders) = await NewTwoTables(factory);
            var customer = new Customer { PartitionKey = "p1", Name = "Alice" };
            var order = new Order { PartitionKey = "p1", CustomerId = "c1", Total = 42m };
            // same id as `customer` -> the second insert violates the primary key at commit time
            var duplicate = new Customer { PartitionKey = "p1", Name = "Clash" };

            var tx = new Transaction();
            await tx.Insert(customers, customer);
            await tx.Insert(orders, order);
            duplicate.id = customer.id;
            await tx.Insert(customers, duplicate);

            await AssertCommitFails(tx);

            Assert.IsNull(await customers.Find("p1", customer.id));
            Assert.IsNull(await orders.Find("p1", order.id));
        }

        // A native rollback restores the row EXACTLY, etag included. Compensation could not do this:
        // it re-wrote the row and thereby handed it a fresh etag.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task FailedCommit_LeavesEtagUntouched(Func<string, Task<IRelationalStore>> factory)
        {
            var (_, customers, orders) = await NewTwoTables(factory);
            var customer = new Customer { PartitionKey = "p1", Name = "original" };
            await customers.Insert(customer);
            string etagBefore = customer.etag;

            var clash = new Order { PartitionKey = "p1", CustomerId = "c1", Total = 1m };
            await orders.Insert(clash);
            var duplicate = new Order { PartitionKey = "p1", CustomerId = "c1", Total = 2m, id = clash.id };

            var tx = new Transaction();
            tx.AddOriginal(customers, customer);
            customer.Name = "modified";
            await tx.Update(customers, customer);
            await tx.Insert(orders, duplicate);   // fails at commit

            await AssertCommitFails(tx);

            var found = await customers.Find("p1", customer.id);
            Assert.AreEqual("original", found.Name);
            Assert.AreEqual(etagBefore, found.etag, "a native ROLLBACK restores the row, etag included");
        }

        // Mixed unit of work: the relational store commits LAST (last-resource). A failure in the
        // compensation-only store therefore costs the relational side a free ROLLBACK.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task MixedUnitOfWork_WhenDocumentStoreFails_RelationalRollsBack(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            IDocumentStore documentStore = new Memory_DocumentStore("");
            var documents = await documentStore.CreateCollection<TxDoc>("txcol");

            var rec = Sample();
            var doc = new TxDoc { PartitionKey = "pk", id = "d1", str_value = "x" };
            var duplicate = new TxDoc { PartitionKey = "pk", id = "d1", str_value = "y" };

            var tx = new Transaction();
            await tx.Insert(table, rec);
            await tx.Insert(documents, doc);
            await tx.Insert(documents, duplicate);   // duplicate id -> throws at commit

            await Assert.ThrowsExceptionAsync<DuplicateKeyException>(() => tx.Commit());

            Assert.IsNull(await table.Find(rec.PartitionKey, rec.id), "relational side must be rolled back");
            Assert.IsNull(await documents.Find("pk", "d1"), "the document that did get written must be compensated");
        }

        // The commit actions are the event-dispatch hook: they run once, after the data is durable.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task CommitActions_RunAfterTheDataIsDurable(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            SampleRecord? seenByCommitAction = null;

            await using (var tx = new Transaction())
            {
                await tx.Insert(table, rec);
                tx.AddCommitAction(async () => seenByCommitAction = await table.Find(rec.PartitionKey, rec.id));
                await tx.Commit();
            }

            Assert.IsNotNull(seenByCommitAction, "a commit action must already see the committed row");
        }

        private static async Task<(IRelationalStore Store, ITable<Customer> Customers, ITable<Order> Orders)> NewTwoTables(
            Func<string, Task<IRelationalStore>> factory)
        {
            var store = await factory(TestMain.NewTableName());
            var customers = await store.CreateTable<Customer>(TestMain.NewTableName());
            var orders = await store.CreateTable<Order>(TestMain.NewTableName());
            return (store, customers, orders);
        }

        // A primary-key violation surfaces as a provider-specific exception (SqliteException /
        // PostgresException), so the type is not asserted - only that the commit did not succeed.
        private static async Task AssertCommitFails(Transaction tx)
        {
            try
            {
                await tx.Commit();
            }
            catch (Exception)
            {
                return;
            }

            Assert.Fail("Commit() was expected to fail");
        }
    }
}
