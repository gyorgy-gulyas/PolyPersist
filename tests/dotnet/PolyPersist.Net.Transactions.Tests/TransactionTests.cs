using PolyPersist;
using PolyPersist.Net.Core;
using PolyPersist.Net.DocumentStore.Memory;
using PolyPersist.Net.Transactions;

namespace PolyPersist.Net.Transactions.Tests
{
    // PP-04: first test coverage for the (optimistic, run-then-compensate) transaction.
    // Uses the in-memory document store, so it needs no external database / Docker.
    public class TxDoc : Entity, IDocument
    {
        public string str_value { get; set; }
    }

    [TestClass]
    public class TransactionTests
    {
        private static async Task<IDocumentCollection<TxDoc>> NewCollectionAsync()
        {
            IDocumentStore store = new Memory_DocumentStore("");
            return await store.CreateCollection<TxDoc>("txcol");
        }

        [TestMethod]
        public async Task Insert_Commit_KeepsDocument()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Commit();

            Assert.IsNotNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Insert_Rollback_RemovesDocument()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Rollback();

            Assert.IsNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Insert_DisposeWithoutCommit_RollsBack()
        {
            var col = await NewCollectionAsync();
            await using (var tx = new Transaction())
            {
                await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            } // leaving the scope without Commit -> auto rollback

            Assert.IsNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Update_Rollback_RestoresOriginal()
        {
            var col = await NewCollectionAsync();
            var doc = new TxDoc { PartitionKey = "pk", id = "a", str_value = "original" };
            await col.Insert(doc);

            var tx = new Transaction();
            tx.AddOriginal(col, doc);
            doc.str_value = "changed";
            await tx.Update(col, doc);
            await tx.Rollback();

            var found = await col.Find("pk", "a");
            Assert.IsNotNull(found);
            Assert.AreEqual("original", found.str_value);
        }

        [TestMethod]
        public async Task Delete_Rollback_ReInsertsDocument()
        {
            var col = await NewCollectionAsync();
            var doc = new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" };
            await col.Insert(doc);

            var tx = new Transaction();
            tx.AddOriginal(col, doc);
            await tx.Delete(col, doc);
            await tx.Rollback();

            Assert.IsNotNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Rollback_AfterCommit_IsNoOp()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Commit();
            await tx.Rollback(); // already committed -> must not undo the insert

            Assert.IsNotNull(await col.Find("pk", "a"));
        }
    }
}
