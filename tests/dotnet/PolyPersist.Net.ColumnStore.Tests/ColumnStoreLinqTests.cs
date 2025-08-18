using PolyPersist.Net.Extensions;
using PolyPersist.Net.Test;
using System.Reflection;

namespace PolyPersist.Net.ColumnStore.Tests
{
    [TestClass]
    [DoNotParallelize]
    public class ColumnStoreLinqTests
    {
        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Where_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "C", int_value = 30 }
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
                .Where(r => r.int_value == 20 )
                .ToList();
            Assert.AreEqual(1, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q2"));
        }

        internal class SomeDTO
        {
            public string value1 { get; set; }
            public string value2 { get; set; }
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Select_Dto_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "C", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var listDto = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .Select(r => new SomeDTO(){ value2 = r.id, value1 = r.str_value })
                .ToList();
            Assert.AreEqual(2, listDto.Count);
            Assert.IsNotNull(listDto.First(r => r.value2 == "q1"));
            Assert.IsNotNull(listDto.First(r => r.value2 == "q2"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Select_NoName_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "C", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var listAnonim = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .Select(r => new { r.id, r.str_value })
                .ToList();
            Assert.AreEqual(2, listAnonim.Count);
            Assert.IsNotNull(listAnonim.First(r => r.id == "q1"));
            Assert.IsNotNull(listAnonim.First(r => r.id == "q2"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Select_SingleColum_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "C", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var listAnonim = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .Select(r => r.id)
                .ToList();
            Assert.AreEqual(2, listAnonim.Count);
            Assert.IsNotNull(listAnonim.First(r => r == "q1"));
            Assert.IsNotNull(listAnonim.First(r => r == "q2"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Select_Disctinct_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "A", int_value = 20 },
                new SampleRow { PartitionKey = "query-pk", id = "q3", str_value = "B", int_value = 30 },
                new SampleRow { PartitionKey = "diff-pk", id = "q4", str_value = "B", int_value = 40 },
            };

            foreach (var r in rows)
                await table.Insert(r);

            List<string> listStr = table
                .AsQueryable()
                .Select(r => r.PartitionKey)
                .Distinct()
                .ToList();
            Assert.AreEqual(2, listStr.Count);
            Assert.IsNotNull(listStr.First(r => r == "query-pk"));
            Assert.IsNotNull(listStr.First(r => r == "diff-pk" ));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Count_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "C", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var count = table
                .AsQueryable()
                .Count();
            Assert.AreEqual(3, count);

            count = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .Count();
            Assert.AreEqual(2, count);
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Take_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "C", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var list = table
                .AsQueryable()
                .Take(1)
                .ToList();
            Assert.AreEqual(1, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q1"));

            list = table
                .AsQueryable()
                .Take(10)
                .ToList();
            Assert.AreEqual(3, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q1"));
            Assert.IsNotNull(list.First(r => r.id == "q2"));
            Assert.IsNotNull(list.First(r => r.id == "q2"));

            list = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .Take(1)
                .ToList();
            Assert.AreEqual(1, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q1"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_Skip_NotSupported(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "A", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "B", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "C", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);


            var exception = Assert.ThrowsException<NotSupportedException>( () =>
            {
                var list = table
                    .AsQueryable()
                    .Skip(1)
                    .ToList();
            });
            Assert.IsTrue(exception.Message.Contains("Skip"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_MemberFunctions_NotSupported(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "ABC", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "BCD", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "CDE", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                var list = table
                   .AsQueryable()
                   .Where(r => r.str_value.ToLower() == "abc")
                   .ToList();
            });
            Assert.IsTrue(exception.Message.Contains("ToLower"));

            exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                var list = table
                   .AsQueryable()
                   .Where(r => r.str_value.ToUpper() == "abc")
                   .ToList();
            });
            Assert.IsTrue(exception.Message.Contains("ToUpper"));

            exception = Assert.ThrowsException<NotSupportedException>(() =>
            {
                var list = table
                   .AsQueryable()
                   .Where(r => r.str_value.Trim() == "abc")
                   .ToList();
            });
            Assert.IsTrue(exception.Message.Contains("Trim"));
        }

        [DataTestMethod]
        [DynamicData(nameof(TestMain.StoreInstances), typeof(TestMain), DynamicDataSourceType.Property)]
        public async Task ColumnStore_Linq_OrderBy_OK(Func<string, Task<IColumnStore>> factory)
        {
            var testName = MethodBase.GetCurrentMethod().GetAsyncMethodName().MakeStorageConformName();
            var store = await factory(testName);
            var table = await store.CreateTable<SampleRow>("sampletable");

            var rows = new[]
            {
                new SampleRow { PartitionKey = "query-pk", id = "q1", str_value = "BCD", int_value = 10 },
                new SampleRow { PartitionKey = "query-pk", id = "q2", str_value = "ABC", int_value = 20 },
                new SampleRow { PartitionKey = "diffe-pk", id = "q3", str_value = "CDE", int_value = 30 }
            };

            foreach (var r in rows)
                await table.Insert(r);

            var list = table
                .AsQueryable()
                .Where( r => r.PartitionKey == "query-pk" )
                .OrderBy( r => r.id )
                .ToList();
            Assert.AreEqual(2, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q1"));
            Assert.IsNotNull(list.First(r => r.id == "q2"));

            list = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .OrderByDescending(r => r.id )
                .ToList();
            Assert.AreEqual(2, list.Count);
            Assert.IsNotNull(list.First(r => r.id == "q2"));
            Assert.IsNotNull(list.First(r => r.id == "q1"));
        }
    }
}