using System.Text;
using PolyPersist;
using PolyPersist.Net.BlobStore.Memory;
using PolyPersist.Net.ColumnStore.Memory;
using PolyPersist.Net.Core;
using PolyPersist.Net.DocumentStore.Memory;
using PolyPersist.Net.Transactions;

namespace PolyPersist.Net.Transactions.Tests
{
    // Additional coverage for the run-then-compensate Transaction coordinator: commit AND
    // rollback for every entity kind (document / row / blob / record) and every verb
    // (Insert / Update / Delete + blob Upload / UpdateContent / UpdateMetadata), plus the
    // guard / error / dispose paths. Docker-free: uses the in-memory stores and an in-memory
    // fake relational table (no in-memory relational store ships in the repo).

    public sealed class TxRow : Entity, IRow
    {
        public string str_value { get; set; } = null!;
    }

    public sealed class TxBlob : Blob
    {
    }

    public sealed class TxRecord : Entity, IRecord
    {
        public string str_value { get; set; } = null!;
    }

    [TestClass]
    public class TransactionCoverageTests
    {
        // ---------------------------------------------------------------- helpers

        private static async Task<IDocumentCollection<TxDoc>> NewCollectionAsync()
        {
            IDocumentStore store = new Memory_DocumentStore("");
            return await store.CreateCollection<TxDoc>("col");
        }

        private static async Task<IColumnTable<TxRow>> NewTableAsync()
        {
            IColumnStore store = new Memory_ColumnStore("");
            return await store.CreateTable<TxRow>("tbl");
        }

        private static async Task<IBlobContainer<TxBlob>> NewContainerAsync()
        {
            IBlobStore store = new Memory_BlobStore("");
            return await store.CreateContainer<TxBlob>("cnt");
        }

        private static Stream S(string text) => new MemoryStream(Encoding.UTF8.GetBytes(text));

        private static async Task<string> ReadContentAsync(IBlobContainer<TxBlob> container, TxBlob blob)
        {
            using var stream = await container.Download(blob);
            using var reader = new StreamReader(stream, Encoding.UTF8);
            return await reader.ReadToEndAsync();
        }

        // ---------------------------------------------------------------- document

        [TestMethod]
        public async Task Document_Update_Commit_Persists()
        {
            var col = await NewCollectionAsync();
            var doc = new TxDoc { PartitionKey = "pk", id = "a", str_value = "orig" };
            await col.Insert(doc);

            var tx = new Transaction();
            tx.AddOriginal(col, doc);
            doc.str_value = "changed";
            await tx.Update(col, doc);
            await tx.Commit();

            var found = await col.Find("pk", "a");
            Assert.AreEqual("changed", found.str_value);
        }

        [TestMethod]
        public async Task Document_Delete_Commit_Removes()
        {
            var col = await NewCollectionAsync();
            var doc = new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" };
            await col.Insert(doc);

            var tx = new Transaction();
            tx.AddOriginal(col, doc);
            await tx.Delete(col, doc);
            await tx.Commit();

            Assert.IsNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Document_AddOriginal_NewEntity_Throws()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            Assert.ThrowsException<InvalidOperationException>(
                () => tx.AddOriginal(col, new TxDoc { PartitionKey = "pk", id = "a" }));
        }

        [TestMethod]
        public async Task Document_AddOriginal_Twice_Throws()
        {
            var col = await NewCollectionAsync();
            var doc = new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" };
            await col.Insert(doc);

            var tx = new Transaction();
            tx.AddOriginal(col, doc);
            Assert.ThrowsException<InvalidOperationException>(() => tx.AddOriginal(col, doc));
        }

        [TestMethod]
        public async Task Document_Update_WithoutOriginal_Throws()
        {
            var col = await NewCollectionAsync();
            var doc = new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" };
            await col.Insert(doc);

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Update(col, doc));
        }

        [TestMethod]
        public async Task Document_Delete_WithoutOriginal_Throws()
        {
            var col = await NewCollectionAsync();
            var doc = new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" };
            await col.Insert(doc);

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Delete(col, doc));
        }

        // ---------------------------------------------------------------- row

        [TestMethod]
        public async Task Row_Insert_Commit_Persists()
        {
            var tbl = await NewTableAsync();
            var tx = new Transaction();
            await tx.Insert(tbl, new TxRow { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Commit();

            Assert.IsNotNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Row_Insert_Rollback_Removes()
        {
            var tbl = await NewTableAsync();
            var tx = new Transaction();
            await tx.Insert(tbl, new TxRow { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Rollback();

            Assert.IsNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Row_Update_Rollback_RestoresOriginal()
        {
            var tbl = await NewTableAsync();
            var row = new TxRow { PartitionKey = "pk", id = "a", str_value = "orig" };
            await tbl.Insert(row);

            var tx = new Transaction();
            tx.AddOriginal(tbl, row);
            row.str_value = "changed";
            await tx.Update(tbl, row);
            await tx.Rollback();

            var found = await tbl.Find("pk", "a");
            Assert.AreEqual("orig", found.str_value);
        }

        [TestMethod]
        public async Task Row_Update_Commit_Persists()
        {
            var tbl = await NewTableAsync();
            var row = new TxRow { PartitionKey = "pk", id = "a", str_value = "orig" };
            await tbl.Insert(row);

            var tx = new Transaction();
            tx.AddOriginal(tbl, row);
            row.str_value = "changed";
            await tx.Update(tbl, row);
            await tx.Commit();

            var found = await tbl.Find("pk", "a");
            Assert.AreEqual("changed", found.str_value);
        }

        [TestMethod]
        public async Task Row_Delete_Rollback_ReInserts()
        {
            var tbl = await NewTableAsync();
            var row = new TxRow { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(row);

            var tx = new Transaction();
            tx.AddOriginal(tbl, row);
            await tx.Delete(tbl, row);
            await tx.Rollback();

            Assert.IsNotNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Row_Delete_Commit_Removes()
        {
            var tbl = await NewTableAsync();
            var row = new TxRow { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(row);

            var tx = new Transaction();
            tx.AddOriginal(tbl, row);
            await tx.Delete(tbl, row);
            await tx.Commit();

            Assert.IsNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Row_AddOriginal_NewEntity_Throws()
        {
            var tbl = await NewTableAsync();
            var tx = new Transaction();
            Assert.ThrowsException<InvalidOperationException>(
                () => tx.AddOriginal(tbl, new TxRow { PartitionKey = "pk", id = "a" }));
        }

        [TestMethod]
        public async Task Row_AddOriginal_Twice_Throws()
        {
            var tbl = await NewTableAsync();
            var row = new TxRow { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(row);

            var tx = new Transaction();
            tx.AddOriginal(tbl, row);
            Assert.ThrowsException<InvalidOperationException>(() => tx.AddOriginal(tbl, row));
        }

        [TestMethod]
        public async Task Row_Update_WithoutOriginal_Throws()
        {
            var tbl = await NewTableAsync();
            var row = new TxRow { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(row);

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Update(tbl, row));
        }

        [TestMethod]
        public async Task Row_Delete_WithoutOriginal_Throws()
        {
            var tbl = await NewTableAsync();
            var row = new TxRow { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(row);

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Delete(tbl, row));
        }

        // ---------------------------------------------------------------- blob

        // Upload is the blob counterpart of Insert: a brand-new blob, no AddOriginal required.
        private static TxBlob NewUploadBlob() =>
            new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };

        [TestMethod]
        public async Task Blob_Upload_Commit_Persists()
        {
            var cnt = await NewContainerAsync();
            var tx = new Transaction();
            await tx.Upload(cnt, NewUploadBlob(), S("hello"));
            await tx.Commit();

            Assert.IsNotNull(await cnt.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Blob_Upload_Rollback_Removes()
        {
            var cnt = await NewContainerAsync();
            var tx = new Transaction();
            await tx.Upload(cnt, NewUploadBlob(), S("hello"));
            await tx.Rollback();

            Assert.IsNull(await cnt.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Blob_UpdateContent_Rollback_RestoresContent()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await tx.AddOriginal(cnt, blob);
            await tx.UpdateContent(cnt, blob, S("v2"));
            // deferred: the new content is buffered, the store still holds the old one
            Assert.AreEqual("v1", await ReadContentAsync(cnt, blob));

            await tx.Rollback();
            Assert.AreEqual("v1", await ReadContentAsync(cnt, blob));
        }

        [TestMethod]
        public async Task Blob_UpdateContent_Commit_Persists()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await tx.AddOriginal(cnt, blob);
            await tx.UpdateContent(cnt, blob, S("v2"));
            await tx.Commit();

            Assert.AreEqual("v2", await ReadContentAsync(cnt, blob));
        }

        [TestMethod]
        public async Task Blob_UpdateMetadata_Rollback_RestoresMetadata()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "orig.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await tx.AddOriginal(cnt, blob);
            blob.fileName = "changed.txt";
            await tx.UpdateMetadata(cnt, blob);
            await tx.Rollback();

            var found = await cnt.Find("pk", "a");
            Assert.AreEqual("orig.txt", found.fileName);
        }

        [TestMethod]
        public async Task Blob_UpdateMetadata_Commit_Persists()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "orig.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await tx.AddOriginal(cnt, blob);
            blob.fileName = "changed.txt";
            await tx.UpdateMetadata(cnt, blob);
            await tx.Commit();

            var found = await cnt.Find("pk", "a");
            Assert.AreEqual("changed.txt", found.fileName);
        }

        [TestMethod]
        public async Task Blob_Delete_Rollback_ReUploadsWithContent()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("payload"));

            var tx = new Transaction();
            await tx.AddOriginal(cnt, blob);
            await tx.Delete(cnt, blob);
            // deferred: the delete is queued, the blob is still there
            Assert.IsNotNull(await cnt.Find("pk", "a"));

            await tx.Rollback();
            var found = await cnt.Find("pk", "a");
            Assert.IsNotNull(found);
            Assert.AreEqual("payload", await ReadContentAsync(cnt, found));
        }

        [TestMethod]
        public async Task Blob_Delete_Commit_Removes()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("payload"));

            var tx = new Transaction();
            await tx.AddOriginal(cnt, blob);
            await tx.Delete(cnt, blob);
            await tx.Commit();

            Assert.IsNull(await cnt.Find("pk", "a"));
        }

        // The upload is only written at Commit(), so the content must be captured when it is queued:
        // the caller is free to close its stream as soon as Upload() returns.
        [TestMethod]
        public async Task Blob_Upload_CapturesContent_SoTheCallerMayCloseTheStream()
        {
            var cnt = await NewContainerAsync();
            var tx = new Transaction();

            var content = S("hello");
            await tx.Upload(cnt, NewUploadBlob(), content);
            await content.DisposeAsync();

            await tx.Commit();

            var found = await cnt.Find("pk", "a");
            Assert.AreEqual("hello", await ReadContentAsync(cnt, found));
        }

        // Content above the in-memory cap spills to a temp file rather than being held in memory
        // until the commit. 2 MiB is comfortably over the 1 MiB threshold.
        [TestMethod]
        public async Task Blob_Upload_LargeContent_SpillsToDisk_AndCommits()
        {
            var cnt = await NewContainerAsync();
            byte[] payload = _LargePayload(2 * 1024 * 1024);

            await using (var tx = new Transaction())
            {
                var content = new MemoryStream(payload);
                await tx.Upload(cnt, NewUploadBlob(), content);
                await content.DisposeAsync();
                await tx.Commit();
            }

            var found = await cnt.Find("pk", "a");
            using var stored = await cnt.Download(found);
            using var buffer = new MemoryStream();
            await stored.CopyToAsync(buffer);

            CollectionAssert.AreEqual(payload, buffer.ToArray());
        }

        // A spilled upload that is never committed writes nothing, and disposing releases the temp file.
        [TestMethod]
        public async Task Blob_Upload_LargeContent_Rollback_WritesNothing()
        {
            var cnt = await NewContainerAsync();

            await using (var tx = new Transaction())
            {
                using var content = new MemoryStream(_LargePayload(2 * 1024 * 1024));
                await tx.Upload(cnt, NewUploadBlob(), content);
                await tx.Rollback();
            }

            Assert.IsNull(await cnt.Find("pk", "a"));
        }

        private static byte[] _LargePayload(int size)
        {
            byte[] payload = new byte[size];
            for (int i = 0; i < payload.Length; i++)
                payload[i] = (byte)(i % 251);   // a prime, so the pattern does not align with the copy buffer

            return payload;
        }

        [TestMethod]
        public async Task Blob_AddOriginal_NewEntity_Throws()
        {
            var cnt = await NewContainerAsync();
            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(
                () => tx.AddOriginal(cnt, new TxBlob { PartitionKey = "pk", id = "a", fileName = "f", contentType = "t" }));
        }

        [TestMethod]
        public async Task Blob_AddOriginal_Twice_Throws()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await tx.AddOriginal(cnt, blob);
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.AddOriginal(cnt, blob));
        }

        [TestMethod]
        public async Task Blob_UpdateContent_WithoutOriginal_Throws()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.UpdateContent(cnt, blob, S("v2")));
        }

        [TestMethod]
        public async Task Blob_UpdateMetadata_WithoutOriginal_Throws()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.UpdateMetadata(cnt, blob));
        }

        [TestMethod]
        public async Task Blob_Delete_WithoutOriginal_Throws()
        {
            var cnt = await NewContainerAsync();
            var blob = new TxBlob { PartitionKey = "pk", id = "a", fileName = "f.txt", contentType = "text/plain" };
            await cnt.Upload(blob, S("v1"));

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Delete(cnt, blob));
        }

        // ---------------------------------------------------------------- record (fake relational table)

        [TestMethod]
        public async Task Record_Insert_Commit_Persists()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var tx = new Transaction();
            await tx.Insert(tbl, new TxRecord { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Commit();

            Assert.IsNotNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Record_Insert_Rollback_Removes()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var tx = new Transaction();
            await tx.Insert(tbl, new TxRecord { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Rollback();

            Assert.IsNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Record_Update_Rollback_RestoresOriginal()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var rec = new TxRecord { PartitionKey = "pk", id = "a", str_value = "orig" };
            await tbl.Insert(rec);

            var tx = new Transaction();
            tx.AddOriginal(tbl, rec);
            rec.str_value = "changed";
            await tx.Update(tbl, rec);
            await tx.Rollback();

            var found = await tbl.Find("pk", "a");
            Assert.AreEqual("orig", found.str_value);
        }

        [TestMethod]
        public async Task Record_Update_Commit_Persists()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var rec = new TxRecord { PartitionKey = "pk", id = "a", str_value = "orig" };
            await tbl.Insert(rec);

            var tx = new Transaction();
            tx.AddOriginal(tbl, rec);
            rec.str_value = "changed";
            await tx.Update(tbl, rec);
            await tx.Commit();

            var found = await tbl.Find("pk", "a");
            Assert.AreEqual("changed", found.str_value);
        }

        [TestMethod]
        public async Task Record_Delete_Rollback_ReInserts()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var rec = new TxRecord { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(rec);

            var tx = new Transaction();
            tx.AddOriginal(tbl, rec);
            await tx.Delete(tbl, rec);
            await tx.Rollback();

            Assert.IsNotNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Record_Delete_Commit_Removes()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var rec = new TxRecord { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(rec);

            var tx = new Transaction();
            tx.AddOriginal(tbl, rec);
            await tx.Delete(tbl, rec);
            await tx.Commit();

            Assert.IsNull(await tbl.Find("pk", "a"));
        }

        [TestMethod]
        public void Record_AddOriginal_NewEntity_Throws()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var tx = new Transaction();
            Assert.ThrowsException<InvalidOperationException>(
                () => tx.AddOriginal(tbl, new TxRecord { PartitionKey = "pk", id = "a" }));
        }

        [TestMethod]
        public async Task Record_AddOriginal_Twice_Throws()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var rec = new TxRecord { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(rec);

            var tx = new Transaction();
            tx.AddOriginal(tbl, rec);
            Assert.ThrowsException<InvalidOperationException>(() => tx.AddOriginal(tbl, rec));
        }

        [TestMethod]
        public async Task Record_Update_WithoutOriginal_Throws()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var rec = new TxRecord { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(rec);

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Update(tbl, rec));
        }

        [TestMethod]
        public async Task Record_Delete_WithoutOriginal_Throws()
        {
            var tbl = new FakeRecordTable<TxRecord>();
            var rec = new TxRecord { PartitionKey = "pk", id = "a", str_value = "x" };
            await tbl.Insert(rec);

            var tx = new Transaction();
            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => tx.Delete(tbl, rec));
        }

        // ---------------------------------------------------------------- cross-cutting

        [TestMethod]
        public async Task Commit_RunsCommitActions()
        {
            var tx = new Transaction();
            bool ran = false;
            tx.AddCommitAction(() => { ran = true; return ValueTask.CompletedTask; });
            await tx.Commit();
            Assert.IsTrue(ran);
        }

        [TestMethod]
        public async Task Rollback_RunsCustomRollBackAction()
        {
            var tx = new Transaction();
            bool ran = false;
            tx.AddRollBackAction(() => { ran = true; return ValueTask.CompletedTask; });
            await tx.Rollback();
            Assert.IsTrue(ran);
        }

        [TestMethod]
        public async Task Commit_Twice_IsNoOp()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Commit();
            await tx.Commit(); // second commit hits the _commitStarted guard and returns

            Assert.IsNotNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task EmptyTransaction_CommitThenRollback_NoThrow()
        {
            var tx = new Transaction();
            await tx.Commit();
            await tx.Rollback(); // committed -> no-op
        }

        [TestMethod]
        public async Task EmptyTransaction_Rollback_NoThrow()
        {
            var tx = new Transaction();
            await tx.Rollback();
        }

        [TestMethod]
        public async Task FailingOperation_TriggersRollback_OfPriorOperations()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "first" });

            try
            {
                // duplicate id -> the memory store throws inside Insert
                await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "dupe" });
                Assert.Fail("expected the duplicate insert to throw");
            }
            catch (Exception)
            {
                await tx.Rollback();
            }

            Assert.IsNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Rollback_WhenACompensationFails_ThrowsAggregate()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            tx.AddRollBackAction(() => throw new InvalidOperationException("rollback boom"));

            await Assert.ThrowsExceptionAsync<AggregateException>(() => tx.Rollback());
        }

        [TestMethod]
        public async Task Dispose_Sync_WithoutCommit_RollsBack()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            tx.Dispose(); // blocking rollback fallback

            Assert.IsNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task Dispose_Sync_Twice_IsNoOp()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            tx.Dispose();
            tx.Dispose(); // second dispose hits the _disposed guard
            Assert.IsNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task DisposeAsync_AfterCommit_DoesNotRollBack()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.Commit();
            await tx.DisposeAsync(); // committed -> must keep the insert

            Assert.IsNotNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task DisposeAsync_Twice_IsNoOp()
        {
            var col = await NewCollectionAsync();
            var tx = new Transaction();
            await tx.Insert(col, new TxDoc { PartitionKey = "pk", id = "a", str_value = "x" });
            await tx.DisposeAsync();
            await tx.DisposeAsync(); // second hits the _disposed guard

            Assert.IsNull(await col.Find("pk", "a"));
        }

        [TestMethod]
        public async Task DisposeAsync_EmptyTransaction_NoRollback()
        {
            var tx = new Transaction();
            await tx.DisposeAsync(); // no executed operations -> nothing to compensate
        }

        // ---------------------------------------------------------------- in-memory fake relational table

        // Minimal in-memory ITable<TRecord> with optimistic-concurrency etag semantics, so the
        // relational overloads of Transaction can be exercised without a real relational store.
        private sealed class FakeRecordTable<TRecord> : ITable<TRecord>
            where TRecord : IRecord, new()
        {
            private sealed record Stored(string PartitionKey, string Etag, string Json);

            private readonly Dictionary<string, Stored> _rows = new();

            public IStore ParentStore => null!;
            public string Name => "fake";

            public Task Insert(TRecord record)
            {
                if (string.IsNullOrEmpty(record.PartitionKey))
                    throw new Exception("PartitionKey must be filled");
                if (string.IsNullOrEmpty(record.etag) == false)
                    throw new Exception("ETag is already filled at Insert");

                if (string.IsNullOrEmpty(record.id))
                    record.id = Guid.NewGuid().ToString();
                else if (_rows.ContainsKey(record.id))
                    throw new Exception($"Record {record.id} duplicate key");

                record.etag = Guid.NewGuid().ToString();
                record.LastUpdate = DateTime.UtcNow;
                _rows[record.id] = new Stored(record.PartitionKey, record.etag,
                    System.Text.Json.JsonSerializer.Serialize(record));
                return Task.CompletedTask;
            }

            public Task Update(TRecord record)
            {
                if (string.IsNullOrEmpty(record.etag))
                    throw new Exception("ETag must be filled at Update");
                if (_rows.TryGetValue(record.id, out var stored) == false)
                    throw new Exception($"Record {record.id} does not exist");
                if (stored.Etag != record.etag)
                    throw new Exception($"Record {record.id} already changed");

                record.etag = Guid.NewGuid().ToString();
                record.LastUpdate = DateTime.UtcNow;
                _rows[record.id] = new Stored(record.PartitionKey, record.etag,
                    System.Text.Json.JsonSerializer.Serialize(record));
                return Task.CompletedTask;
            }

            public Task Delete(string partitionKey, string id)
            {
                if (_rows.TryGetValue(id, out var stored) == false || stored.PartitionKey != partitionKey)
                    throw new Exception($"Record {id} already removed");
                _rows.Remove(id);
                return Task.CompletedTask;
            }

            public Task<TRecord> Find(string partitionKey, string id)
            {
                if (_rows.TryGetValue(id, out var stored) && stored.PartitionKey == partitionKey)
                    return Task.FromResult(System.Text.Json.JsonSerializer.Deserialize<TRecord>(stored.Json)!);
                return Task.FromResult(default(TRecord)!);
            }

            public System.Linq.IQueryable<TRecord> Query(string partitionKey) =>
                QueryCrossPartition().Where(r => r.PartitionKey == partitionKey);
            public System.Linq.IQueryable<TRecord> QueryCrossPartition() =>
                _rows.Values.Select(s => System.Text.Json.JsonSerializer.Deserialize<TRecord>(s.Json)!).AsQueryable();

            public object GetUnderlyingImplementation() => _rows;
        }
    }
}
