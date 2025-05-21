using PolyPersist.Net.Extensions;
using PolyPersist.Net.Test;
using System;
using System.Reflection;

namespace PolyPersist.Net.ColumnStore.Tests
{
    [TestClass]
    [DoNotParallelize]
    public class ColumnTableTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_GetUnderlyingImplementation_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            Assert.IsNotNull(table.Name);
            Assert.IsNotNull(table.GetUnderlyingImplementation());
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Insert_And_Find_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var row = new SampleRow
            {
                PartitionKey = "pk1",
                id = Guid.NewGuid().ToString(),
                str_value = "hello",
                int_value = 42,
                decimal_value = 12.34m,
                bool_value = true,
                date_value = DateOnly.Parse("2024-05-01"),
                time_value = TimeOnly.Parse("15:45"),
                datetime_value = DateTime.UtcNow
            };

            await table.Insert(row);

            var found = await table.Find(row.PartitionKey, row.id);
            Assert.IsNotNull(found);
            Assert.AreEqual(row.str_value, found.str_value);
            Assert.AreEqual(row.int_value, found.int_value);
            Assert.AreEqual(row.decimal_value, found.decimal_value);
            Assert.AreEqual(row.bool_value, found.bool_value);
            Assert.AreEqual(row.date_value, found.date_value);
            Assert.AreEqual(row.time_value, found.time_value);
            Assert.AreEqual(row.datetime_value.ToString("s"), found.datetime_value.ToString("s")); // rounded compare
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Insert_Duplicate_Fails(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var id = Guid.NewGuid().ToString();
            var row1 = new SampleRow { PartitionKey = "dup", id = id, str_value = "first" };
            var row2 = new SampleRow { PartitionKey = "dup", id = id, str_value = "second" };

            await table.Insert(row1);

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await table.Insert(row2);
            });
            Assert.IsTrue(exception.Message.Contains("duplicate key"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Update_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("mytable23");

            var id = Guid.NewGuid().ToString();
            id = "myid001";
            var row = new SampleRow { PartitionKey = "pk2", id = id, str_value = "initial" };
            await table.Insert(row);

            row.str_value = "updated";
            await table.Update(row);

            var updated = await table.Find(row.PartitionKey, row.id);
            Assert.IsNotNull(updated);
            Assert.AreEqual("updated", updated.str_value);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Update_NotFound_Fails(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var missing = new SampleRow { PartitionKey = "notfound", id = "missing", etag = "fake-etag", str_value = "data" };

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await table.Update(missing);
            });
            Assert.IsTrue(exception.Message.Contains("does not exist"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Delete_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var row = new SampleRow { PartitionKey = "pk3", id = Guid.NewGuid().ToString(), str_value = "to delete" };
            await table.Insert(row);
            await table.Delete(row.PartitionKey, row.id);

            var deleted = await table.Find(row.PartitionKey, row.id);
            Assert.IsNull(deleted);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Delete_NotFound_Fails(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var exception = await Assert.ThrowsExceptionAsync<Exception>(async () =>
            {
                await table.Delete("missing-pk", "missing-id");
            });
            Assert.IsTrue(exception.Message.Contains("already removed"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Find_NotFound_ReturnsNull(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var result = await table.Find("notfound", "nope");
            Assert.IsNull(result);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Query_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q2", str_value = "C", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var list = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .ToList();
            Assert.AreEqual(2, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q1"));
            Assert.IsNotNull(list.First(r => r.id == "q2"));

            list = table
             .AsQueryable()
             .Where(r => r.int_value > 10 && r.int_value < 30 )
             .ToList();
            Assert.AreEqual(1, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q2"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Insert_AssignsEtagAndLastUpdate(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var row = new SampleRow { PartitionKey = "pk", id = "r1" };
            await table.Insert(row);

            var loaded = await table.Find("pk", "r1");

            Assert.IsFalse(string.IsNullOrEmpty(loaded.etag));
            Assert.IsTrue(loaded.LastUpdate > DateTime.MinValue);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Update_ChangesEtagAndLastUpdate(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var row = new SampleRow { PartitionKey = "pk", id = "r1", str_value = "before" };
            await table.Insert(row);
            var original = await table.Find("pk", "r1");

            await Task.Delay(10); // ensure timestamp change
            row.str_value = "after";
            await table.Update(row);

            var updated = await table.Find("pk", "r1");
            Assert.AreNotEqual(original.etag, updated.etag);
            Assert.IsTrue(updated.LastUpdate > original.LastUpdate);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Insert_DistinctEtagsForDistinctRows(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var r1 = new SampleRow { PartitionKey = "pk", id = "r1" };
            var r2 = new SampleRow { PartitionKey = "pk", id = "r2" };

            await table.Insert(r1);
            await table.Insert(r2);

            var loaded1 = await table.Find("pk", "r1");
            var loaded2 = await table.Find("pk", "r2");

            Assert.AreNotEqual(loaded1.etag, loaded2.etag);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnTable_Insert_EtagAndLastUpdate_NotEmpty(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var row = new SampleRow { PartitionKey = "pk", id = "r1" };
            await table.Insert(row);
            var found = await table.Find("pk", "r1");

            Assert.IsFalse(string.IsNullOrEmpty(found.etag));
            Assert.IsTrue(found.LastUpdate > DateTime.MinValue);
        }
    }
}