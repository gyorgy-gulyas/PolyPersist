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
        public string str_value { get; set; } = null!;
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

        // Writes are deferred to Commit(), so abandoning a transaction never reaches the store:
        // there is nothing to compensate, and the spy sees neither an insert nor a delete.
        [TestMethod]
        public async Task Rollback_BeforeCommit_TouchesNoStore()
        {
            var spy = new RecordingCollection<TxDoc>();
            var tx = new Transaction();
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "A" });
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "B" });

            await tx.Rollback();

            CollectionAssert.AreEqual(Array.Empty<string>(), spy.InsertOrder);
            CollectionAssert.AreEqual(Array.Empty<string>(), spy.DeleteOrder);
        }

        // PP-10: when a commit fails halfway, the operations that DID reach a compensation-only store
        // must be undone sequentially in REVERSE order (LIFO), so a later operation is compensated
        // before the earlier one it may depend on.
        [TestMethod]
        public async Task Commit_WhenAnOperationFails_RunsCompensationsInReverseOrder()
        {
            var spy = new RecordingCollection<TxDoc> { FailInsertOfId = "D" };
            var tx = new Transaction();
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "A" });
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "B" });
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "C" });
            await tx.Insert(spy, new TxDoc { PartitionKey = "pk", id = "D" });

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Commit());

            CollectionAssert.AreEqual(new[] { "A", "B", "C" }, spy.InsertOrder);
            CollectionAssert.AreEqual(new[] { "C", "B", "A" }, spy.DeleteOrder);
        }

        // A commit action runs only after the data is durably committed, so its failure surfaces as
        // an exception but must NOT undo the data - and the follow-up Rollback() is a no-op.
        [TestMethod]
        public async Task Commit_WhenCommitActionFails_DataStaysCommitted()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            tx.AddCommitAction(() => throw new InvalidOperationException("commit boom"));

            await Assert.ThrowsExceptionAsync<AggregateException>(() => tx.Commit());

            Assert.IsNotNull(await col.Find("pk", "a"));

            await tx.Rollback(); // already committed -> must not undo the insert
            Assert.IsNotNull(await col.Find("pk", "a"));
        }

        // Commit actions are the event-dispatch hook, so they must run in registration order.
        [TestMethod]
        public async Task Commit_RunsCommitActionsInRegistrationOrder()
        {
            var order = new List<string>();
            var tx = new Transaction();
            tx.AddCommitAction(() => { order.Add("first"); return ValueTask.CompletedTask; });
            tx.AddCommitAction(() => { order.Add("second"); return ValueTask.CompletedTask; });
            tx.AddCommitAction(() => { order.Add("third"); return ValueTask.CompletedTask; });

            await tx.Commit();

            CollectionAssert.AreEqual(new[] { "first", "second", "third" }, order);
        }

        // records the order of Insert and Delete calls (Delete is the Insert compensation)
        private sealed class RecordingCollection<TDoc> : IDocumentCollection<TDoc> where TDoc : IDocument, new()
        {
            public readonly List<string> InsertOrder = new();
            public readonly List<string> DeleteOrder = new();
            /// <summary>When set, inserting this id throws - to make a commit fail halfway.</summary>
            public string? FailInsertOfId { get; init; }

            public IStore ParentStore => null!;
            public string Name => "spy";
            public Task Insert(TDoc document)
            {
                if (document.id == FailInsertOfId)
                    throw new InvalidOperationException($"insert of '{document.id}' boom");

                InsertOrder.Add(document.id);
                return Task.CompletedTask;
            }
            public Task Update(TDoc document) => Task.CompletedTask;
            public Task Delete(string partitionKey, string id) { DeleteOrder.Add(id); return Task.CompletedTask; }
            public Task<TDoc> Find(string partitionKey, string id) => Task.FromResult(default(TDoc))!;
            public System.Linq.IQueryable<TDoc> Query(string partitionKey) => null!;
            public System.Linq.IQueryable<TDoc> QueryCrossPartition() => null!;
            public object GetUnderlyingImplementation() => null!;
        }
    }
}
