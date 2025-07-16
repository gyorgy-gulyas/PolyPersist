using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace PolyPersist.Net.Transactions
{
    /// <summary>
    /// Represents a transactional context for document, row, and blob operations.
    /// Tracks changes, supports rollback and commit semantics, and provides deep clones of entities for recovery.
    /// </summary>
    public class Transaction : IAsyncDisposable, IDisposable
    {
        /// <summary>
        /// Defines supported transactional operations.
        /// </summary>
        public enum Operations
        {
            Insert,
            Update,
            Delete
        }

        // Holds deep clone JSON and optional blob content
        private readonly record struct _EntityData(string Json, ReadOnlyMemory<byte> Content);
        // Holds deep-cloned state of tracked entities (JSON + blob content)
        // Reason: multiple threads might AddOriginal concurrently in rare cases. eg: Task.WhenAll( ... ) for multiple objects
        private readonly ConcurrentDictionary<string, _EntityData> _deepCloneOfEntities = [];
        // List of rollback actions to revert state if the transaction is aborted
        // Reason: rollback actions might be added from different threads. eg: Task.WhenAll( ... ) for multiple objects
        private readonly ConcurrentBag<Func<ValueTask>> _rollBackActions = [];
        // List of commit actions to finalize state when the transaction succeeds
        // Reason: commit actions might be added from different threads. eg: Task.WhenAll( ... ) for multiple objects
        private readonly ConcurrentBag<Func<ValueTask>> _commitActions = [];
        // Tracks executed operations for auditing or diagnostics
        // Reason: multiple threads might Update/Insert concurrently in rare cases. eg: Task.WhenAll( ... ) for multiple objects
        private readonly ConcurrentBag<(Operations, IEntity)> _executedOperations = [];

        // Tracks whether Dispose/DisposeAsync was called(Interlocked used for thread-safety).
        private int _disposed;
        // Tracks whether Commit() succeeded. Volatile for lock-free checks.
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

            if (_deepCloneOfEntities.TryAdd(key, new _EntityData(JsonSerializer.Serialize(document), ReadOnlyMemory<byte>.Empty)) == false)
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
            if (_deepCloneOfEntities.TryAdd(key, new _EntityData(JsonSerializer.Serialize(row), ReadOnlyMemory<byte>.Empty)) == false)
                throw new InvalidOperationException($"Row: '{row.GetType().FullName}' already added. Id: '{row.id}'");
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
        public async ValueTask AddOriginal<TBlob>(IBlobContainer<TBlob> container, TBlob blob)
            where TBlob : IBlob, new()
        {
            if (string.IsNullOrWhiteSpace(blob.etag))
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' is a new entity, cannot be added as Original. Id: '{blob.id}'");

            string key = _getEntityKey(blob);
            using var stream = await container.Download(blob).ConfigureAwait(false);
            var buffer = await _readAllBytesAsync(stream).ConfigureAwait(false);

            if (_deepCloneOfEntities.TryAdd(key, new _EntityData(JsonSerializer.Serialize(blob), buffer)) == false)
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' already added. Id: '{blob.id}'");
        }

        /// <summary>
        /// Inserts a new document and registers a rollback action to delete it if needed.
        /// </summary>
        /// <typeparam name="TDocument">The type of the document entity.</typeparam>
        /// <param name="collection">The document collection to insert into.</param>
        /// <param name="document">The document instance to insert.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        public async ValueTask Insert<TDocument>(IDocumentCollection<TDocument> collection, TDocument document)
            where TDocument : IDocument, new()
        {
            await collection.Insert(document).ConfigureAwait(false);
            _executedOperations.Add((Operations.Insert, document));

            _rollBackActions.Add(async () =>
            {
                await collection.Delete(document.PartitionKey, document.id).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Inserts a new row and registers a rollback action to delete it if needed.
        /// </summary>
        /// <typeparam name="TRow">The type of the row entity.</typeparam>
        /// <param name="table">The table to insert into.</param>
        /// <param name="row">The row instance to insert.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        public async ValueTask Insert<TRow>(IColumnTable<TRow> table, TRow row)
            where TRow : IRow, new()
        {
            await table.Insert(row).ConfigureAwait(false);
            _executedOperations.Add((Operations.Insert, row));

            _rollBackActions.Add(async () =>
            {
                await table.Delete(row.PartitionKey, row.id).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Updates an existing document and registers a rollback action to restore its original state if needed.
        /// </summary>
        /// <typeparam name="TDocument">The type of the document entity.</typeparam>
        /// <param name="collection">The document collection containing the entity.</param>
        /// <param name="document">The updated document instance.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the document was not added as Original before update.
        /// </exception>
        public async ValueTask Update<TDocument>(IDocumentCollection<TDocument> collection, TDocument document)
            where TDocument : IDocument, new()
        {
            string key = _getEntityKey(document);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Document: '{document.GetType().FullName}' not added as Original. Id: '{document.id}'");

            await collection.Update(document).ConfigureAwait(false);
            _executedOperations.Add((Operations.Update, document));

            _rollBackActions.Add(async () =>
            {
                TDocument original = JsonSerializer.Deserialize<TDocument>(_deepCloneOfEntities[key].Json);
                await collection.Update(original).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Updates an existing row and registers a rollback action to restore its original state if needed.
        /// </summary>
        /// <typeparam name="TRow">The type of the row entity.</typeparam>
        /// <param name="table">The table containing the entity.</param>
        /// <param name="row">The updated row instance.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the row was not added as Original before update.
        /// </exception>
        public async ValueTask Update<TRow>(IColumnTable<TRow> table, TRow row)
            where TRow : IRow, new()
        {
            string key = _getEntityKey(row);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Row: '{row.GetType().FullName}' not added as Original. Id: '{row.id}'");

            await table.Update(row).ConfigureAwait(false);
            _executedOperations.Add((Operations.Update, row));

            _rollBackActions.Add(async () =>
            {
                TRow original = JsonSerializer.Deserialize<TRow>(_deepCloneOfEntities[key].Json);
                await table.Update(original).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Uploads a new blob and registers a rollback action to delete it if needed.
        /// </summary>
        /// <typeparam name="TBlob">The type of the blob entity.</typeparam>
        /// <param name="container">The blob container to upload into.</param>
        /// <param name="blob">The blob metadata.</param>
        /// <param name="content">The content stream of the blob.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob was not added as Original before upload.
        /// </exception>
        public async ValueTask Upload<TBlob>(IBlobContainer<TBlob> container, TBlob blob, Stream content)
            where TBlob : IBlob, new()
        {
            string key = _getEntityKey(blob);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' not added as Original. Id: '{blob.id}'");

            await container.Upload(blob, content).ConfigureAwait(false);
            _executedOperations.Add((Operations.Insert, blob));

            _rollBackActions.Add(async () =>
            {
                await container.Delete(blob.PartitionKey, blob.id).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Updates the content of an existing blob and registers a rollback action to restore its original content if needed.
        /// </summary>
        /// <typeparam name="TBlob">The type of the blob entity.</typeparam>
        /// <param name="container">The blob container containing the entity.</param>
        /// <param name="blob">The blob metadata.</param>
        /// <param name="content">The new content stream for the blob.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob was not added as Original before content update.
        /// </exception>
        public async ValueTask UpdateContent<TBlob>(IBlobContainer<TBlob> container, TBlob blob, Stream content)
            where TBlob : IBlob, new()
        {
            string key = _getEntityKey(blob);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' not added as Original. Id: '{blob.id}'");

            await container.UpdateContent(blob, content).ConfigureAwait(false);
            _executedOperations.Add((Operations.Update, blob));

            _rollBackActions.Add(async () =>
            {
                var originalContent = _deepCloneOfEntities[key].Content;
                using var ms = new MemoryStream(originalContent.ToArray(), writable: false);
                await container.UpdateContent(blob, ms).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Updates the metadata of an existing blob and registers a rollback action to restore its original metadata if needed.
        /// </summary>
        /// <typeparam name="TBlob">The type of the blob entity.</typeparam>
        /// <param name="container">The blob container containing the entity.</param>
        /// <param name="blob">The updated blob metadata.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob was not added as Original before metadata update.
        /// </exception>
        public async ValueTask UpdateMetadata<TBlob>(IBlobContainer<TBlob> container, TBlob blob)
            where TBlob : IBlob, new()
        {
            string key = _getEntityKey(blob);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' not added as Original. Id: '{blob.id}'");

            await container.UpdateMetadata(blob).ConfigureAwait(false);
            _executedOperations.Add((Operations.Update, blob));

            _rollBackActions.Add(async () =>
            {
                TBlob original = JsonSerializer.Deserialize<TBlob>(_deepCloneOfEntities[key].Json);
                await container.UpdateMetadata(original).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Deletes an existing document and registers a rollback action to re-insert its original state if needed.
        /// </summary>
        /// <typeparam name="TDocument">The type of the document entity.</typeparam>
        /// <param name="collection">The document collection containing the entity.</param>
        /// <param name="document">The document instance to delete.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the document was not added as Original before deletion.
        /// </exception>
        public async ValueTask Delete<TDocument>(IDocumentCollection<TDocument> collection, TDocument document)
            where TDocument : IDocument, new()
        {
            string key = _getEntityKey(document);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Document: '{document.GetType().FullName}' not added as Original. Id: '{document.id}'");

            await collection.Delete(document.PartitionKey, document.id).ConfigureAwait(false);
            _executedOperations.Add((Operations.Delete, document));

            _rollBackActions.Add(async () =>
            {
                TDocument original = JsonSerializer.Deserialize<TDocument>(_deepCloneOfEntities[key].Json);
                await collection.Insert(original).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Deletes an existing row and registers a rollback action to re-insert its original state if needed.
        /// </summary>
        /// <typeparam name="TRow">The type of the row entity.</typeparam>
        /// <param name="table">The table containing the entity.</param>
        /// <param name="row">The row instance to delete.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the row was not added as Original before deletion.
        /// </exception>
        public async ValueTask Delete<TRow>(IColumnTable<TRow> table, TRow row)
            where TRow : IRow, new()
        {
            string key = _getEntityKey(row);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Row: '{row.GetType().FullName}' not added as Original. Id: '{row.id}'");

            await table.Delete(row.PartitionKey, row.id).ConfigureAwait(false);
            _executedOperations.Add((Operations.Delete, row));

            _rollBackActions.Add(async () =>
            {
                TRow original = JsonSerializer.Deserialize<TRow>(_deepCloneOfEntities[key].Json);
                await table.Insert(original).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Deletes an existing blob and registers a rollback action to re-upload its original state if needed.
        /// </summary>
        /// <typeparam name="TBlob">The type of the blob entity.</typeparam>
        /// <param name="container">The blob container containing the entity.</param>
        /// <param name="blob">The blob metadata to delete.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown if the blob was not added as Original before deletion.
        /// </exception>
        public async ValueTask Delete<TBlob>(IBlobContainer<TBlob> container, TBlob blob)
            where TBlob : IBlob, new()
        {
            string key = _getEntityKey(blob);
            if (!_deepCloneOfEntities.ContainsKey(key))
                throw new InvalidOperationException($"Blob: '{blob.GetType().FullName}' not added as Original. Id: '{blob.id}'");

            await container.Delete(blob.PartitionKey, blob.id).ConfigureAwait(false);
            _executedOperations.Add((Operations.Delete, blob));

            _rollBackActions.Add(async () =>
            {
                TBlob original = JsonSerializer.Deserialize<TBlob>(_deepCloneOfEntities[key].Json);
                var originalContent = _deepCloneOfEntities[key].Content;
                using var ms = new MemoryStream(originalContent.Span.ToArray(), writable: false);
                await container.Upload(original, ms).ConfigureAwait(false);
            });
        }

        /// <summary>
        /// Adds a custom commit action to the transaction.
        /// </summary>
        /// <param name="action">The asynchronous action to perform during commit.</param>
        public void AddCommitAction(Func<ValueTask> action) => _commitActions.Add(action);


        /// <summary>
        /// Commits the transaction by executing all registered commit actions in parallel.
        /// </summary>
        /// <returns>A task that represents the asynchronous commit operation.</returns>
        public async ValueTask Commit()
        {
            // is already committed ?
            if (Interlocked.Exchange(ref _committed, 1) == 1)
                return;

            await _ExecuteSafely(_commitActions).ConfigureAwait(false);
            _Clear();
        }

        /// <summary>
        /// Adds a custom rollback action to the transaction.
        /// </summary>
        /// <param name="action">The asynchronous action to perform during rollback.</param>
        public void AddRollBackAction(Func<ValueTask> action) => _rollBackActions.Add(action);

        /// <summary>
        /// Rolls back the transaction by executing all registered rollback actions in parallel.
        /// </summary>
        /// <returns>A task that represents the asynchronous rollback operation.</returns>
        public async ValueTask Rollback()
        {
            // is already committed ?
            if (Volatile.Read(ref _committed) == 1)
                return;

            await _ExecuteSafely(_rollBackActions.Reverse()).ConfigureAwait(false);
            _Clear();
        }

        // _ExecuteSafely runs all actions concurrently and collects exceptions.
        // Decision: aggregate all failures into AggregateException instead of failing fast.
        // Reason: Allows all rollback steps to be attempted even if some fail.
        private static async Task _ExecuteSafely(IEnumerable<Func<ValueTask>> actions, [CallerMemberName] string function = "")
        {
            var tasks = actions.Select(async action =>
            {
                try
                {
                    await action().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    return ex;
                }
                return null;
            });

            var results = await Task.WhenAll(tasks).ConfigureAwait(false);
            var exceptions = results.Where(ex => ex != null).Cast<Exception>().ToList();

            if (exceptions.Any())
                throw new AggregateException($"One or more actions failed in {function}.", exceptions);
        }

        // Reads all bytes from a stream into ReadOnlyMemory<byte>
        private static async Task<ReadOnlyMemory<byte>> _readAllBytesAsync(Stream stream)
        {
            // Decision: _readAllBytesAsync uses MemoryStream.GetBuffer + AsMemory.
            // Reason: efficient zero-copy of data; safe because we trim to actual length.
            using var ms = new MemoryStream();
            await stream.CopyToAsync(ms).ConfigureAwait(false);
            var buffer = ms.GetBuffer();
            return buffer.AsMemory(0, (int)ms.Length);
        }

        // Generates a unique key for tracking an entity
        private static string _getEntityKey(IEntity entity)
            => $"{entity.GetType().Name}_{entity.id}";

        // Clears all tracked state from the transaction
        private void _Clear()
        {
            _executedOperations.Clear();
            _deepCloneOfEntities.Clear();
            _rollBackActions.Clear();
            _commitActions.Clear();
        }

        #region IDisposable
        /// <summary>
        /// Asynchronously disposes the transaction.
        /// If not committed, rolls back all pending changes.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            // Decision: Dispose fallback to blocking rollback if not committed.
            // Reason: ensures resources aren't leaked even if Dispose called synchronously.
            // Known risk: potential deadlock if rollback involves async I/O on sync context.

            // is already disposed ?
            // Decision: use Interlocked.Exchange to guard Commit() and Dispose() against double execution.
            // Reason: ensures idempotency even under race conditions.
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
                return;

            if (Volatile.Read(ref _committed) == 0 && _executedOperations.Any())
            {
                await Rollback().ConfigureAwait(false);
            }
        }
        /// <summary>
        /// Disposes the transaction, clearing all state and suppressing finalization.
        /// </summary>
        public void Dispose()
        {
            // is already disposed ?
            // Decision: use Interlocked.Exchange to guard Commit() and Dispose() against double execution.
            // Reason: ensures idempotency even under race conditions.
            if (Interlocked.Exchange(ref _disposed, 1) == 1)
                return;

            // Decision: Dispose fallback to blocking rollback if not committed.
            // Reason: ensures resources aren't leaked even if Dispose called synchronously.
            if (Volatile.Read(ref _committed) == 0 && _executedOperations.Any())
            {
                // Blocking fallback: not recommended for I/O
                Rollback().AsTask().GetAwaiter().GetResult();
            }

            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
