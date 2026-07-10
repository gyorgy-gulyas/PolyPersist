using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.Transactions
{
    /// <summary>
    /// A unit of work over document, row, record and blob operations.
    /// <para>
    /// Nothing is written until <see cref="Commit"/>: every operation is queued, in order, and
    /// replayed there. That makes a rollback before the commit free and exact - no compensation runs,
    /// because nothing ever reached a store and no reader ever saw a half-finished change.
    /// </para>
    /// <para>
    /// Inside <see cref="Commit"/> a store that can offer a native database transaction
    /// (<see cref="ITransactionParticipant"/>, today the relational store) gets one, shared by all of
    /// its tables, and it commits LAST - after every compensation-only store has succeeded. So a
    /// failure anywhere costs that store a free ROLLBACK instead of a compensation, and only the
    /// stores that cannot do better are compensated. With more than one native participant the
    /// guarantee degrades to "all but one commit atomically"; there is no two-phase commit.
    /// </para>
    /// <para>
    /// A store used WITHOUT a transaction writes immediately, as it always did.
    /// </para>
    /// </summary>
    public class Transaction : ITransaction, IAsyncDisposable, IDisposable
    {
        // Blob content larger than this is spilled to a temp file instead of being held in memory
        // until the commit. A transaction may buffer several uploads at once, so the cap is per blob
        // and deliberately small.
        private const int _MaxInMemoryContentBytes = 1024 * 1024;

        // A queued operation, plus the store it belongs to: the store decides whether the operation
        // can run inside a native transaction scope at commit time.
        private readonly record struct _PendingOperation(IStore Store, Func<ITransactionScope?, Task> Execute);

        // Queued operations, in call order. Commit() replays them in exactly this order.
        // Concurrent enqueue is supported (eg. Task.WhenAll over several entities).
        private readonly ConcurrentQueue<_PendingOperation> _pendingOperations = new();

        // Holds deep clone JSON and, for blobs, the original content
        private readonly record struct _EntityData(string Json, _ContentBuffer? Content);
        // Holds deep-cloned state of tracked entities (JSON + blob content)
        // Reason: multiple threads might AddOriginal concurrently in rare cases. eg: Task.WhenAll( ... ) for multiple objects
        private readonly ConcurrentDictionary<string, _EntityData> _deepCloneOfEntities = [];
        // Content captured for a queued Upload / UpdateContent, released when the transaction ends.
        private readonly ConcurrentQueue<_ContentBuffer> _queuedContents = new();
        // Compensations for operations that were already applied during a commit to a store that has
        // no native transaction. Empty until Commit() starts, and empty again unless a commit failed
        // halfway. A queue (not a bag) so insertion order is preserved: rollback runs them in reverse
        // (LIFO) so later operations are compensated before the ones they may depend on.
        private readonly ConcurrentQueue<Func<ValueTask>> _rollBackActions = new();
        // Actions to run once the data is durably committed - the place to dispatch events from.
        // A queue, not a bag: the order in which they were registered is the order they run in.
        private readonly ConcurrentQueue<Func<ValueTask>> _commitActions = new();
        // Tracks executed operations for auditing or diagnostics
        // Reason: multiple threads might Update/Insert concurrently in rare cases. eg: Task.WhenAll( ... ) for multiple objects
        private readonly ConcurrentBag<(ITransaction.Operations, IEntity)> _executedOperations = [];

        // Tracks whether Dispose/DisposeAsync was called(Interlocked used for thread-safety).
        private int _disposed;
        // Tracks whether a Commit() has started (guards against double/concurrent commit).
        private int _commitStarted;
        // Tracks whether the DATA was committed. Volatile for lock-free checks. Set only after the
        // stores have committed, so a failed commit does not block Rollback().
        private int _committed;

        /// <summary>
        /// Adds an existing document to the transaction for change tracking.
        /// </summary>
        /// <typeparam name="TDocument">The type of the document entity.</typeparam>
        /// <param name="collection">The document collection the entity belongs to.</param>
        /// <param name="document">The document instance to track.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the document is a new entity (no etag) or already tracked.
        /// </exception>
        public void AddOriginal<TDocument>(IDocumentCollection<TDocument> collection, TDocument document)
            where TDocument : IDocument, new()
        {
            if (string.IsNullOrWhiteSpace(document.etag))
                throw new InvalidOperationException($"Document: '{document.GetType().FullName}' is a new entity, cannot be added as Original. Id: '{document.id}'");

            string key = _getEntityKey(document);

            if (_deepCloneOfEntities.TryAdd(key, new _EntityData(JsonSerializer.Serialize(document), null)) == false)
                throw new InvalidOperationException($"Document: '{document.GetType().FullName}' already added. Id: '{document.id}'");
        }

        /// <summary>
        /// Adds an existing row to the transaction for change tracking.
        /// </summary>
        /// <typeparam name="TRow">The type of the row entity.</typeparam>
        /// <param name="table">The table the row belongs to.</param>
        /// <param name="row">The row instance to track.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the row is a new entity (no etag) or already tracked.
        /// </exception>
        public void AddOriginal<TRow>(IColumnTable<TRow> table, TRow row)
            where TRow : IRow, new()
        {
            if (string.IsNullOrWhiteSpace(row.etag))
                throw new InvalidOperationException($"Row: '{row.GetType().FullName}' is a new entity, cannot be added as Original. Id: '{row.id}'");

            string key = _getEntityKey(row);
            if (_deepCloneOfEntities.TryAdd(key, new _EntityData(JsonSerializer.Serialize(row), null)) == false)
                throw new InvalidOperationException($"Row: '{row.GetType().FullName}' already added. Id: '{row.id}'");
        }

        /// <summary>
        /// Adds an existing relational row to the transaction for change tracking.
        /// </summary>
        public void AddOriginal<TRecord>(ITable<TRecord> table, TRecord record)
            where TRecord : IRecord, new()
        {
            if (string.IsNullOrWhiteSpace(record.etag))
                throw new InvalidOperationException($"Record: '{record.GetType().FullName}' is a new entity, cannot be added as Original. Id: '{record.id}'");

            string key = _getEntityKey(record);
            if (_deepCloneOfEntities.TryAdd(key, new _EntityData(JsonSerializer.Serialize(record), null)) == false)
                throw new InvalidOperationException($"Record: '{record.GetType().FullName}' already added. Id: '{record.id}'");
        }

        /// <summary>
        /// Adds an existing blob to the transaction for change tracking,
        /// including its content snapshot.
        /// </summary>
        /// <typeparam name="TBlob">The type of the blob entity.</typeparam>
        /// <param name="container">The blob container the blob belongs to.</param>
        /// <param name="blob">The blob instance to track.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob is a new entity (no etag) or already tracked.
        /// </exception>
        public async Task AddOriginal<TBlob>(IBlobContainer<TBlob> container, TBlob blob)
            where TBlob : IBlob, new()
        {
            if (string.IsNullOrWhiteSpace(blob.etag))
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' is a new entity, cannot be added as Original. Id: '{blob.id}'");

            string key = _getEntityKey(blob);
            using var stream = await container.Download(blob).ConfigureAwait(false);
            var content = await _ContentBuffer.Capture(stream).ConfigureAwait(false);

            if (_deepCloneOfEntities.TryAdd(key, new _EntityData(JsonSerializer.Serialize(blob), content)) == false)
            {
                await content.DisposeAsync().ConfigureAwait(false);
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' already added. Id: '{blob.id}'");
            }
        }

        /// <summary>
        /// Queues the insert of a new document. The document receives its id right away; the write
        /// itself, and the rollback action that would delete it, happen at Commit().
        /// </summary>
        public Task Insert<TDocument>(IDocumentCollection<TDocument> collection, TDocument document)
            where TDocument : IDocument, new()
        {
            CollectionCommon.CheckBeforeInsert(document);
            CollectionCommon.AssignIdIfMissing(document);

            _Enqueue(collection.ParentStore, async _ =>
            {
                await collection.Insert(document).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Insert, document));

                _rollBackActions.Enqueue(async () =>
                {
                    await collection.Delete(document.PartitionKey, document.id).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues the insert of a new row. The row receives its id right away; the write itself, and
        /// the rollback action that would delete it, happen at Commit().
        /// </summary>
        public Task Insert<TRow>(IColumnTable<TRow> table, TRow row)
            where TRow : IRow, new()
        {
            CollectionCommon.CheckBeforeInsert(row);
            CollectionCommon.AssignIdIfMissing(row);

            _Enqueue(table.ParentStore, async _ =>
            {
                await table.Insert(row).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Insert, row));

                _rollBackActions.Enqueue(async () =>
                {
                    await table.Delete(row.PartitionKey, row.id).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues the insert of a new relational row. No compensation is registered: the row is
        /// written inside the store's native transaction, which a failure simply rolls back.
        /// </summary>
        public Task Insert<TRecord>(ITable<TRecord> table, TRecord record)
            where TRecord : IRecord, new()
        {
            CollectionCommon.CheckBeforeInsert(record);
            CollectionCommon.AssignIdIfMissing(record);

            _Enqueue(table.ParentStore, async scope =>
            {
                await _Bind(scope, table).Insert(record).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Insert, record));
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues the upload of a new blob. The content is captured now - the caller may close the
        /// stream as soon as this returns - and replayed at Commit().
        /// </summary>
        public async Task Upload<TBlob>(IBlobContainer<TBlob> container, TBlob blob, Stream content)
            where TBlob : IBlob, new()
        {
            // Upload is the blob counterpart of Insert: it creates a NEW blob, so there is no
            // original to track (AddOriginal would even be impossible - a new blob has no etag and
            // cannot be downloaded). Rollback simply deletes what was uploaded.
            CollectionCommon.CheckBeforeInsert(blob);
            CollectionCommon.AssignIdIfMissing(blob);

            var captured = await _CaptureContent(content).ConfigureAwait(false);

            _Enqueue(container.ParentStore, async _ =>
            {
                using var stream = captured.OpenRead();
                await container.Upload(blob, stream).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Insert, blob));

                _rollBackActions.Enqueue(async () =>
                {
                    await container.Delete(blob.PartitionKey, blob.id).ConfigureAwait(false);
                });
            });
        }

        /// <summary>
        /// Queues an update of an existing document; the rollback action restores the tracked original.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the document was not added as Original before update.
        /// </exception>
        public Task Update<TDocument>(IDocumentCollection<TDocument> collection, TDocument document)
            where TDocument : IDocument, new()
        {
            string key = _RequireOriginal(document, "Document");
            CollectionCommon.CheckBeforeUpdate(document);

            _Enqueue(collection.ParentStore, async _ =>
            {
                await collection.Update(document).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Update, document));

                _rollBackActions.Enqueue(async () =>
                {
                    TDocument original = JsonSerializer.Deserialize<TDocument>(_deepCloneOfEntities[key].Json)!;
                    original.etag = document.etag;   // adopt the current etag so the optimistic-concurrency check passes
                    await collection.Update(original).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues an update of an existing row; the rollback action restores the tracked original.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the row was not added as Original before update.
        /// </exception>
        public Task Update<TRow>(IColumnTable<TRow> table, TRow row)
            where TRow : IRow, new()
        {
            string key = _RequireOriginal(row, "Row");
            CollectionCommon.CheckBeforeUpdate(row);

            _Enqueue(table.ParentStore, async _ =>
            {
                await table.Update(row).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Update, row));

                _rollBackActions.Enqueue(async () =>
                {
                    TRow original = JsonSerializer.Deserialize<TRow>(_deepCloneOfEntities[key].Json)!;
                    original.etag = row.etag;   // adopt the current etag so the optimistic-concurrency check passes
                    await table.Update(original).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues an update of an existing relational row. No compensation: the native transaction
        /// rolls the row back to exactly its previous state, etag included.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the record was not added as Original before update.
        /// </exception>
        public Task Update<TRecord>(ITable<TRecord> table, TRecord record)
            where TRecord : IRecord, new()
        {
            _RequireOriginal(record, "Record");
            CollectionCommon.CheckBeforeUpdate(record);

            _Enqueue(table.ParentStore, async scope =>
            {
                await _Bind(scope, table).Update(record).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Update, record));
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues a content update of an existing blob. The new content is captured now; the rollback
        /// action restores the original content snapshot taken by AddOriginal.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob was not added as Original before content update.
        /// </exception>
        public async Task UpdateContent<TBlob>(IBlobContainer<TBlob> container, TBlob blob, Stream content)
            where TBlob : IBlob, new()
        {
            string key = _RequireOriginal(blob, "Blob");

            var captured = await _CaptureContent(content).ConfigureAwait(false);

            _Enqueue(container.ParentStore, async _ =>
            {
                using var stream = captured.OpenRead();
                await container.UpdateContent(blob, stream).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Update, blob));

                _rollBackActions.Enqueue(async () =>
                {
                    using var original = _deepCloneOfEntities[key].Content!.OpenRead();
                    await container.UpdateContent(blob, original).ConfigureAwait(false);
                });
            });
        }

        /// <summary>
        /// Queues a metadata update of an existing blob; the rollback action restores the original.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob was not added as Original before metadata update.
        /// </exception>
        public Task UpdateMetadata<TBlob>(IBlobContainer<TBlob> container, TBlob blob)
            where TBlob : IBlob, new()
        {
            string key = _RequireOriginal(blob, "Blob");

            _Enqueue(container.ParentStore, async _ =>
            {
                await container.UpdateMetadata(blob).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Update, blob));

                _rollBackActions.Enqueue(async () =>
                {
                    TBlob original = JsonSerializer.Deserialize<TBlob>(_deepCloneOfEntities[key].Json)!;
                    original.etag = blob.etag;   // adopt the current etag so the optimistic-concurrency check passes
                    await container.UpdateMetadata(original).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues the delete of an existing document; the rollback action re-inserts the original.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the document was not added as Original before deletion.
        /// </exception>
        public Task Delete<TDocument>(IDocumentCollection<TDocument> collection, TDocument document)
            where TDocument : IDocument, new()
        {
            string key = _RequireOriginal(document, "Document");

            _Enqueue(collection.ParentStore, async _ =>
            {
                await collection.Delete(document.PartitionKey, document.id).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Delete, document));

                _rollBackActions.Enqueue(async () =>
                {
                    TDocument original = JsonSerializer.Deserialize<TDocument>(_deepCloneOfEntities[key].Json)!;
                    original.etag = null;   // Insert requires an empty etag; it assigns a fresh one
                    await collection.Insert(original).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues the delete of an existing row; the rollback action re-inserts the original.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the row was not added as Original before deletion.
        /// </exception>
        public Task Delete<TRow>(IColumnTable<TRow> table, TRow row)
            where TRow : IRow, new()
        {
            string key = _RequireOriginal(row, "Row");

            _Enqueue(table.ParentStore, async _ =>
            {
                await table.Delete(row.PartitionKey, row.id).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Delete, row));

                _rollBackActions.Enqueue(async () =>
                {
                    TRow original = JsonSerializer.Deserialize<TRow>(_deepCloneOfEntities[key].Json)!;
                    original.etag = null;   // Insert requires an empty etag; it assigns a fresh one
                    await table.Insert(original).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues the delete of an existing relational row. No compensation: the native transaction
        /// restores the row exactly, so its etag survives a rollback.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the record was not added as Original before deletion.
        /// </exception>
        public Task Delete<TRecord>(ITable<TRecord> table, TRecord record)
            where TRecord : IRecord, new()
        {
            _RequireOriginal(record, "Record");

            _Enqueue(table.ParentStore, async scope =>
            {
                await _Bind(scope, table).Delete(record.PartitionKey, record.id).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Delete, record));
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Queues the delete of an existing blob; the rollback action re-uploads the original.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob was not added as Original before deletion.
        /// </exception>
        public Task Delete<TBlob>(IBlobContainer<TBlob> container, TBlob blob)
            where TBlob : IBlob, new()
        {
            string key = _RequireOriginal(blob, "Blob");

            _Enqueue(container.ParentStore, async _ =>
            {
                await container.Delete(blob.PartitionKey, blob.id).ConfigureAwait(false);
                _executedOperations.Add((ITransaction.Operations.Delete, blob));

                _rollBackActions.Enqueue(async () =>
                {
                    TBlob original = JsonSerializer.Deserialize<TBlob>(_deepCloneOfEntities[key].Json)!;
                    original.etag = null;   // Upload requires an empty etag; it assigns a fresh one
                    using var content = _deepCloneOfEntities[key].Content!.OpenRead();
                    await container.Upload(original, content).ConfigureAwait(false);
                });
            });

            return Task.CompletedTask;
        }

        /// <summary>
        /// Adds an action to run once the data has been durably committed - the place to dispatch
        /// events from. A failing commit action surfaces as an exception but never undoes the data,
        /// which is already committed by then.
        /// </summary>
        /// <param name="action">The asynchronous action to perform after the commit.</param>
        public void AddCommitAction(Func<ValueTask> action) => _commitActions.Enqueue(action);

        /// <summary>
        /// Writes everything the transaction has collected, then runs the commit actions.
        /// </summary>
        /// <returns>A task that represents the asynchronous commit operation.</returns>
        public async Task Commit()
        {
            // guard against double / concurrent Commit
            if (Interlocked.Exchange(ref _commitStarted, 1) == 1)
                return;

            // One native transaction per participating store, opened on first use and committed last.
            var scopes = new Dictionary<IStore, ITransactionScope>();
            try
            {
                while (_pendingOperations.TryDequeue(out var operation))
                {
                    ITransactionScope? scope = await _ScopeFor(operation.Store, scopes).ConfigureAwait(false);
                    await operation.Execute(scope).ConfigureAwait(false);
                }

                // Last-resource: by now every compensation-only store has succeeded, so the only thing
                // that can still fail is a database COMMIT.
                foreach (var scope in scopes.Values)
                    await scope.Commit().ConfigureAwait(false);
            }
            catch (Exception commitError)
            {
                // A native participant rolls back for free and exactly; the rest must be compensated.
                foreach (var scope in scopes.Values)
                {
                    try { await scope.Rollback().ConfigureAwait(false); }
                    catch { /* best effort: the compensations below still have to run */ }
                }

                try
                {
                    await _ExecuteSafely(_rollBackActions.Reverse()).ConfigureAwait(false);
                }
                catch (Exception compensationError)
                {
                    // Surface both: the compensation failure alone would hide why the commit failed.
                    throw new AggregateException(
                        "The commit failed and one or more compensations failed as well.",
                        commitError, compensationError);
                }
                finally
                {
                    await _DisposeScopes(scopes).ConfigureAwait(false);
                    await _ClearAsync().ConfigureAwait(false);
                }

                throw;
            }

            await _DisposeScopes(scopes).ConfigureAwait(false);

            // The data is durable from here on: nothing below may roll it back.
            Volatile.Write(ref _committed, 1);

            try { await _ExecuteSafely(_commitActions).ConfigureAwait(false); }
            finally { await _ClearAsync().ConfigureAwait(false); }
        }

        /// <summary>
        /// Adds a custom rollback action to the transaction.
        /// </summary>
        /// <param name="action">The asynchronous action to perform during rollback.</param>
        public void AddRollBackAction(Func<ValueTask> action) => _rollBackActions.Enqueue(action);

        /// <summary>
        /// Abandons the transaction. Before a commit this only drops the queued operations - nothing
        /// was written, so there is nothing to undo. After a commit failed halfway it compensates the
        /// operations that had already been applied.
        /// </summary>
        /// <returns>A task that represents the asynchronous rollback operation.</returns>
        public async Task Rollback()
        {
            // is already committed ?
            if (Volatile.Read(ref _committed) == 1)
                return;

            try { await _ExecuteSafely(_rollBackActions.Reverse()).ConfigureAwait(false); }
            finally { await _ClearAsync().ConfigureAwait(false); }
        }

        // Queues an operation together with the store that owns it.
        private void _Enqueue(IStore store, Func<ITransactionScope?, Task> execute)
            => _pendingOperations.Enqueue(new _PendingOperation(store, execute));

        // Opens (once per store) the native transaction of a store that supports one.
        private static async Task<ITransactionScope?> _ScopeFor(IStore store, Dictionary<IStore, ITransactionScope> scopes)
        {
            if (store is not ITransactionParticipant participant)
                return null;

            if (scopes.TryGetValue(store, out ITransactionScope? scope) == false)
            {
                scope = await participant.BeginScope().ConfigureAwait(false);
                scopes.Add(store, scope);
            }

            return scope;
        }

        // Rebinds a relational table onto the scope's connection so its write joins the native
        // transaction. Without a scope the table writes on its own connection, as it does outside a
        // transaction.
        private static ITable<TRecord> _Bind<TRecord>(ITransactionScope? scope, ITable<TRecord> table)
            where TRecord : IRecord, new()
            => scope is null ? table : scope.Bind(table);

        private static async Task _DisposeScopes(Dictionary<IStore, ITransactionScope> scopes)
        {
            foreach (var scope in scopes.Values)
                await scope.DisposeAsync().ConfigureAwait(false);

            scopes.Clear();
        }

        // Every mutation demands that the caller first registered the entity's original state, so a
        // compensating store has something to restore.
        private string _RequireOriginal<TEntity>(TEntity entity, string kind)
            where TEntity : IEntity
        {
            string key = _getEntityKey(entity);
            if (_deepCloneOfEntities.ContainsKey(key) == false)
                throw new InvalidOperationException($"{kind}: '{entity.GetType().FullName}' not added as Original. Id: '{entity.id}'");

            return key;
        }

        private async Task<_ContentBuffer> _CaptureContent(Stream content)
        {
            var captured = await _ContentBuffer.Capture(content).ConfigureAwait(false);
            _queuedContents.Enqueue(captured);
            return captured;
        }

        // _ExecuteSafely runs all actions and collects exceptions.
        // Decision: aggregate all failures into AggregateException instead of failing fast.
        // Reason: Allows all rollback steps to be attempted even if some fail.
        private static async Task _ExecuteSafely(IEnumerable<Func<ValueTask>> actions, [CallerMemberName] string function = "")
        {
            // Run sequentially in the given order (Rollback passes them reversed), so a later
            // operation is compensated before the earlier one it may depend on. Best-effort:
            // keep going after a failure and aggregate the errors at the end.
            var exceptions = new List<Exception>();
            foreach (var action in actions)
            {
                try
                {
                    await action().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            }

            if (exceptions.Any())
                throw new AggregateException($"One or more actions failed in {function}.", exceptions);
        }

        // Generates a unique key for tracking an entity
        private static string _getEntityKey(IEntity entity)
            => $"{entity.GetType().Name}_{entity.id}";

        // Clears all tracked state, releasing any content spilled to disk.
        private async Task _ClearAsync()
        {
            _pendingOperations.Clear();
            _executedOperations.Clear();
            _rollBackActions.Clear();
            _commitActions.Clear();

            foreach (var data in _deepCloneOfEntities.Values)
            {
                if (data.Content is not null)
                    await data.Content.DisposeAsync().ConfigureAwait(false);
            }
            _deepCloneOfEntities.Clear();

            while (_queuedContents.TryDequeue(out var content))
                await content.DisposeAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Blob content held until the commit. Small payloads stay in memory; anything larger spills
        /// to a temp file, so buffering a big upload does not cost the process that much memory.
        /// </summary>
        private sealed class _ContentBuffer : IAsyncDisposable
        {
            private readonly byte[]? _bytes;
            private readonly string? _path;

            private _ContentBuffer(byte[]? bytes, string? path)
            {
                _bytes = bytes;
                _path = path;
            }

            /// <summary>Reads the stream fully - the caller may close it as soon as this returns.</summary>
            public static async Task<_ContentBuffer> Capture(Stream content)
            {
                var memory = new MemoryStream();
                byte[] chunk = new byte[81920];

                int read;
                while ((read = await content.ReadAsync(chunk).ConfigureAwait(false)) > 0)
                {
                    memory.Write(chunk, 0, read);
                    if (memory.Length <= _MaxInMemoryContentBytes)
                        continue;

                    // Too big to keep: move what we have to disk and stream the rest straight there.
                    string path = Path.GetTempFileName();
                    try
                    {
                        using (var file = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true))
                        {
                            memory.Position = 0;
                            await memory.CopyToAsync(file).ConfigureAwait(false);
                            await content.CopyToAsync(file).ConfigureAwait(false);
                        }
                    }
                    catch
                    {
                        File.Delete(path);
                        throw;
                    }

                    return new _ContentBuffer(null, path);
                }

                return new _ContentBuffer(memory.ToArray(), null);
            }

            /// <summary>A fresh readable stream over the captured content; the caller disposes it.</summary>
            public Stream OpenRead()
                => _bytes is not null
                    ? new MemoryStream(_bytes, writable: false)
                    : new FileStream(_path!, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, useAsync: true);

            public ValueTask DisposeAsync()
            {
                if (_path is not null && File.Exists(_path))
                    File.Delete(_path);

                return ValueTask.CompletedTask;
            }
        }

        #region IDisposable
        /// <summary>
        /// Asynchronously disposes the transaction.
        /// If not committed, rolls back all pending changes.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            // Decision: use Interlocked.Exchange to guard Commit() and Dispose() against double execution.
            // Reason: ensures idempotency even under race conditions.
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
                return;

            // Only a commit that failed halfway leaves anything to compensate; a transaction dropped
            // before its commit merely has queued operations, which _ClearAsync discards.
            if (Volatile.Read(ref _committed) == 0 && _executedOperations.IsEmpty == false)
                await (this as ITransaction).Rollback().ConfigureAwait(false);

            await _ClearAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Disposes the transaction, clearing all state and suppressing finalization.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
                return;

            if (Volatile.Read(ref _committed) == 0 && _executedOperations.IsEmpty == false)
            {
                // Blocking fallback (prefer `await using` / DisposeAsync). Offload to the
                // thread pool so the async rollback does not capture a caller
                // SynchronizationContext, which would deadlock this blocking wait.
                Task.Run(async () => await (this as ITransaction).Rollback().ConfigureAwait(false)).GetAwaiter().GetResult();
            }

            Task.Run(async () => await _ClearAsync().ConfigureAwait(false)).GetAwaiter().GetResult();

            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
