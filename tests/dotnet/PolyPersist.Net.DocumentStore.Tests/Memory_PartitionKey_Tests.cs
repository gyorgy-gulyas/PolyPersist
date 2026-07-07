using PolyPersist;
using PolyPersist.Net.DocumentStore.Memory;
using System;
using System.Threading.Tasks;

namespace PolyPersist.Net.DocumentStore.Tests
{
    // PP-22: the in-memory document collection Delete must respect the partitionKey
    // (it used to match on id alone). Runs against the in-memory store, so no Docker.
    [TestClass]
    public class Memory_PartitionKey_Tests
    {
        [TestMethod]
        public async Task Delete_RespectsPartitionKey()
        {
            IDocumentStore store = new Memory_DocumentStore("");
            var col = await store.CreateCollection<SampleDocument>("ppcol");
            await col.Insert(new SampleDocument { PartitionKey = "p1", id = "a", str_value = "x" });

            await Assert.ThrowsExceptionAsync<Exception>(async () => await col.Delete("p2", "a"));
            Assert.IsNotNull(await col.Find("p1", "a")); // wrong-partition delete left it intact

            await col.Delete("p1", "a"); // right partition -> removed
            Assert.IsNull(await col.Find("p1", "a"));
        }
    }
}
