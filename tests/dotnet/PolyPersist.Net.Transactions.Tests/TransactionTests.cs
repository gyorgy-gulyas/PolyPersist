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

        // PP-10: rollback must run the compensations sequentially in REVERSE order (LIFO),
        // so a later operation is undone before the earlier one it may depend on.
        [TestMethod]
        public async Task Rollback_RunsCompensationsInReverseOrder()
        {
            var spy = new RecordingCollection<TxDoc>();
            var tx = new Transaction();
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "A" });
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "B" });
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "C" });

            await tx.Rollback();

            CollectionAssert.AreEqual(new[] { "C", "B", "A" }, spy.DeleteOrder);
        }

        // PP-11: a failing commit action must NOT mark the transaction committed, so the
        // already-executed operations can still be rolled back (compensation not blocked).
        [TestMethod]
        public async Task Commit_WhenCommitActionFails_StillAllowsRollback()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            tx.AddCommitAction(() => throw new InvalidOperationException("commit boom"));

            await Assert.ThrowsExceptionAsync<AggregateException>(() => tx.Commit());

            await tx.Rollback(); // must still compensate the insert
            Assert.IsNull(await col.Find("pk", "a"));
        }

        // records the order of Delete calls (the Insert compensation); other members are no-ops
        private sealed class RecordingCollection<TDoc> : IDocumentCollection<TDoc> where TDoc : IDocument, new()
        {
            public readonly List<string> DeleteOrder = new();
            public IStore ParentStore => null;
            public string Name => "spy";
            public Task Insert(TDoc document) => Task.CompletedTask;
            public Task Update(TDoc document) => Task.CompletedTask;
            public Task Delete(string partitionKey, string id) { DeleteOrder.Add(id); return Task.CompletedTask; }
            public Task<TDoc> Find(string partitionKey, string id) => Task.FromResult(default(TDoc));
            public System.Linq.IQueryable<TDoc> Query() => null;
            public object GetUnderlyingImplementation() => null;
        }
    }
}
