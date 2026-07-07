using PolyPersist;
using PolyPersist.Net.ColumnStore.Memory;
using System;
using System.Threading.Tasks;

namespace PolyPersist.Net.ColumnStore.Tests
{
    // PP-15: the in-memory column table must key Find/Delete on (partitionKey, id), not id
    // alone. Runs against the in-memory store, so it needs no Docker.
    [TestClass]
    public class Memory_PartitionKey_Tests
    {
        [TestMethod]
        public async Task Find_RespectsPartitionKey()
        {
            IColumnStore store = new Memory_ColumnStore("");
            var table = await store.CreateTable<SampleRow>("pptable");
            await table.Insert(new SampleRow { PartitionKey = "p1", id = "a", str_value = "x" });

            Assert.IsNotNull(await table.Find("p1", "a")); // right partition -> found
            Assert.IsNull(await table.Find("p2", "a"));    // wrong partition -> not found
        }

        [TestMethod]
        public async Task Delete_RespectsPartitionKey()
        {
            IColumnStore store = new Memory_ColumnStore("");
            var table = await store.CreateTable<SampleRow>("pptable");
            await table.Insert(new SampleRow { PartitionKey = "p1", id = "a", str_value = "x" });

            await Assert.ThrowsExceptionAsync<Exception>(async () => await table.Delete("p2", "a"));
            Assert.IsNotNull(await table.Find("p1", "a")); // wrong-partition delete left it intact
        }
    }
}
