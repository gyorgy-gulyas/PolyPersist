using System.Data.Common;
using Dapper;
using LinqToDB;
using LinqToDB.Data;
using PolyPersist.Net.RelationalStore.Dapper;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.RelationalStore.Tests
{
    [TestClass]
    [DoNotParallelize]
    public class RelationalTableTests
    {
        private static async Task<ITable<SampleRecord>> NewTable(Func<string, Task<IRelationalStore>> factory)
        {
            var name = TestMain.NewTableName();
            var store = await factory(name);
            return await store.CreateTable<SampleRecord>(name);
        }

        private static SampleRecord Sample(string pk = "p1", string name = "Alice", int age = 30, decimal balance = 100.5m)
            => new() { PartitionKey = pk, Name = name, Age = age, Balance = balance };

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_Find_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();

            await table.Insert(rec);

            Assert.IsFalse(string.IsNullOrEmpty(rec.id));
            Assert.IsFalse(string.IsNullOrEmpty(rec.etag));
            Assert.AreNotEqual(default, rec.LastUpdate);

            var found = await table.Find("p1", rec.id);
            Assert.IsNotNull(found);
            Assert.AreEqual("Alice", found.Name);
            Assert.AreEqual(30, found.Age);
            Assert.AreEqual(100.5m, found.Balance);
            Assert.AreEqual(rec.etag, found.etag);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_WithoutPartitionKey_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample(pk: null!);
            await Assert.ThrowsExceptionAsync<InvalidRequestException>(() => table.Insert(rec));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_WithEtag_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            rec.etag = Guid.NewGuid().ToString(); // pretend it is already stored
            await Assert.ThrowsExceptionAsync<InvalidRequestException>(() => table.Insert(rec));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_DuplicateId_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            await table.Insert(rec); // id assigned here

            // Reusing the same id must be rejected by the id uniqueness constraint (PP-36); without
            // it the row would just be inserted a second time. The database reports this in its own
            // way (SqliteException / PostgresException), but the contract's type is what callers see
            // (PP-57), and the native error is kept as the inner exception.
            var dup = Sample(name: "Bob");
            dup.id = rec.id;

            var ex = await Assert.ThrowsExceptionAsync<DuplicateKeyException>(() => table.Insert(dup));
            Assert.IsNotNull(ex.InnerException, "the native provider error must be preserved");
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_ViolatingUniqueIndex_Throws_DuplicateKey(Func<string, Task<IRelationalStore>> factory)
        {
            var name = TestMain.NewTableName();
            var store = await factory(name);
            var table = await store.CreateTable<SampleRecord>(name);

            // A unique index on a plain column: the collision is not on the primary key, so this
            // exercises the classification itself rather than linq2db's own key handling.
            using (var db = (DataConnection)table.GetUnderlyingImplementation())
                await db.ExecuteAsync($"CREATE UNIQUE INDEX \"ux_{name}_name\" ON \"{name}\" (\"Name\")");

            await table.Insert(Sample(name: "Alice"));

            await Assert.ThrowsExceptionAsync<DuplicateKeyException>(() => table.Insert(Sample(name: "Alice")));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Insert_ViolatingCheckConstraint_Throws_InvalidRequest(Func<string, Task<IRelationalStore>> factory)
        {
            // Not every constraint failure is a duplicate key: a check violation must land on
            // InvalidRequestException, not DuplicateKeyException. The table is created by hand
            // because a CHECK cannot be expressed through the record type.
            var name = TestMain.NewTableName();
            var store = await factory(name);
            var scratch = await store.CreateTable<SampleRecord>(TestMain.NewTableName());

            using (var db = (DataConnection)scratch.GetUnderlyingImplementation())
                await db.ExecuteAsync(
                    $"CREATE TABLE \"{name}\" (" +
                    "\"id\" varchar(100) NOT NULL PRIMARY KEY, \"PartitionKey\" varchar(100), " +
                    "\"etag\" varchar(100), \"LastUpdate\" timestamp, \"Name\" varchar(100), " +
                    "\"Age\" integer, \"Balance\" decimal(18,5), CHECK (\"Age\" >= 0))");

            var table = await store.GetTableByName<SampleRecord>(name);

            var ex = await Assert.ThrowsExceptionAsync<InvalidRequestException>(() => table.Insert(Sample(age: -1)));
            Assert.IsNotNull(ex.InnerException, "the native provider error must be preserved");
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Find_Missing_ReturnsNull(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var found = await table.Find("p1", Guid.NewGuid().ToString());
            Assert.IsNull(found);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Find_WrongPartition_ReturnsNull(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample(pk: "p1");
            await table.Insert(rec);

            // same id but a different partition key is not the row
            Assert.IsNull(await table.Find("other", rec.id));
            Assert.IsNotNull(await table.Find("p1", rec.id));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Update_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            await table.Insert(rec);
            string firstEtag = rec.etag;

            rec.Name = "Bob";
            rec.Balance = 250m;
            await table.Update(rec);

            Assert.AreNotEqual(firstEtag, rec.etag); // etag rotates on update

            var found = await table.Find("p1", rec.id);
            Assert.IsNotNull(found);
            Assert.AreEqual("Bob", found.Name);
            Assert.AreEqual(250m, found.Balance);
            Assert.AreEqual(rec.etag, found.etag);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Update_StaleEtag_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            await table.Insert(rec);

            // a stale copy still holding the original etag while the row moved on
            var stale = await table.Find("p1", rec.id);
            Assert.IsNotNull(stale);
            rec.Name = "changed";
            await table.Update(rec); // rotates the stored etag

            stale.Name = "conflict";
            var ex = await Assert.ThrowsExceptionAsync<ConcurrencyConflictException>(() => table.Update(stale));
            Assert.IsTrue(ex.Message.Contains("already changed"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Update_Missing_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            rec.id = Guid.NewGuid().ToString();
            rec.etag = Guid.NewGuid().ToString(); // looks like an existing entity, but it is not stored
            var ex = await Assert.ThrowsExceptionAsync<NotFoundException>(() => table.Update(rec));
            Assert.IsTrue(ex.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Delete_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var rec = Sample();
            await table.Insert(rec);

            await table.Delete("p1", rec.id);
            Assert.IsNull(await table.Find("p1", rec.id));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Delete_Missing_Throws(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            var ex = await Assert.ThrowsExceptionAsync<NotFoundException>(() => table.Delete("p1", Guid.NewGuid().ToString()));
            Assert.IsTrue(ex.Message.Contains("already removed"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_Filter_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            await table.Insert(Sample(pk: "p1", name: "young", age: 20));
            await table.Insert(Sample(pk: "p1", name: "mid", age: 40));
            await table.Insert(Sample(pk: "p1", name: "old", age: 60));

            var query = table.Query("p1");
            var adults = query.Where(r => r.Age >= 40).OrderBy(r => r.Age).ToList();

            Assert.AreEqual(2, adults.Count);
            Assert.AreEqual("mid", adults[0].Name);
            Assert.AreEqual("old", adults[1].Name);
        }

        // PP-55: Query(partitionKey) is pre-filtered to that partition (a caller cannot widen it
        // back); QueryCrossPartition() spans every partition. Proves the partition-safe default.
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task Query_ScopedToPartition_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var table = await NewTable(factory);
            await table.Insert(Sample(pk: "p1", name: "a1"));
            await table.Insert(Sample(pk: "p1", name: "a2"));
            await table.Insert(Sample(pk: "p2", name: "b1"));

            // scoped: only the requested partition is visible, even without an explicit .Where
            var p1 = table.Query("p1").ToList();
            Assert.AreEqual(2, p1.Count);
            CollectionAssert.AreEquivalent(new[] { "a1", "a2" }, p1.Select(r => r.Name).ToList());

            // a foreign partition filter on top of a scoped query yields nothing, not a leak
            Assert.AreEqual(0, table.Query("p1").Where(r => r.PartitionKey == "p2").Count());

            // explicit cross-partition spans everything
            Assert.AreEqual(3, table.QueryCrossPartition().Count());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task GetUnderlyingImplementation_RawSql_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var name = TestMain.NewTableName();
            var store = await factory(name);
            var table = await store.CreateTable<SampleRecord>(name);
            await table.Insert(Sample());
            await table.Insert(Sample(name: "second"));

            // GetUnderlyingImplementation returns a linq2db DataConnection; run raw SQL via Dapper.
            using var db = (DataConnection)table.GetUnderlyingImplementation();
            DbConnection conn = (DbConnection)db.Connection;
            long count = await conn.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM {name}");
            Assert.AreEqual(2L, count);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task StoreQuery_Join_Ok(Func<string, Task<IRelationalStore>> factory)
        {
            var customersName = TestMain.NewTableName();
            var ordersName = TestMain.NewTableName();
            var store = await factory(customersName);

            var customers = await store.CreateTable<Customer>(customersName);
            var orders = await store.CreateTable<Order>(ordersName);

            var alice = new Customer { PartitionKey = "p1", Name = "Alice" };
            await customers.Insert(alice);
            await orders.Insert(new Order { PartitionKey = "p1", CustomerId = alice.id, Total = 10m });
            await orders.Insert(new Order { PartitionKey = "p1", CustomerId = alice.id, Total = 20m });

            // Store-level Query() returns the linq2db DataConnection; write a LINQ join across tables.
            using var db = (DataConnection)orders.GetUnderlyingImplementation();
            var joined = await (
                from o in db.GetTable<Order>().TableName(ordersName)
                join c in db.GetTable<Customer>().TableName(customersName) on o.CustomerId equals c.id
                where c.Name == "Alice"
                select o.Total).ToListAsync();

            Assert.AreEqual(2, joined.Count);
            Assert.AreEqual(30m, joined.Sum());
        }
    }
}
