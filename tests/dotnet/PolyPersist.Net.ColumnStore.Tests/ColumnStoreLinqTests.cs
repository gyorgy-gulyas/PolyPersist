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
        public async Task ColumnStore_Linq_Where(Func<string, Task<IColumnStore>> factory)
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
                .Where(r => r.int_value > 10 && r.int_value < 30)
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
        public async Task ColumnStore_Linq_Select_NoName(Func<string, Task<IColumnStore>> factory)
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


        var listDto = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .Select(r => new SomeDTO(){ value2 = r.id, value1 = r.str_value })
                .ToList();
            Assert.AreEqual(2, listDto.Count);
            Assert.IsNotNull(listDto.First(r => r.value2 == "q1"));
            Assert.IsNotNull(listDto.First(r => r.value2 == "q2"));

            var listAnonim = table
                .AsQueryable()
                .Where(r => r.PartitionKey == "query-pk")
                .Select(r => new { r.id, r.str_value })
                .ToList();
            Assert.AreEqual(2, listAnonim.Count);
            Assert.IsNotNull(listAnonim.First(r => r.id == "q1"));
            Assert.IsNotNull(listAnonim.First(r => r.id == "q2"));
        }
    }
}