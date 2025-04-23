using Google.Cloud.BigQuery.V2;
using PolyPersist.Net.Common;
using PolyPersist.Net.Core;

namespace PolyPersist.Net.ColumnStore.GoogleBigQuery
{
    internal class GoogleBigQuery_ColumnTable<TRow> : IColumnTable<TRow>
        where TRow : IRow, new()
    {
        private readonly GoogleBigQuery_ColumnStore _store;
        private readonly string _tableId;

        public GoogleBigQuery_ColumnTable(GoogleBigQuery_ColumnStore store, string tableId)
        {
            _store = store;
            _tableId = tableId;
        }

        string IColumnTable<TRow>.Name => _tableId;

        async Task IColumnTable<TRow>.Insert(TRow row)
        {
            await CollectionCommon.CheckBeforeInsert(row).ConfigureAwait(false);
            row.etag = Guid.NewGuid().ToString();

            await _InsertInternal(row).ConfigureAwait(false);
        }

        async Task IColumnTable<TRow>.Update(TRow row)
        {
            await CollectionCommon.CheckBeforeUpdate(row).ConfigureAwait(false);

            var original = await _FindInternal(row.PartitionKey, row.id).ConfigureAwait(false);
            if (original == null)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} can not be updated because it is already removed.");

            if (row.etag != original.etag)
                throw new Exception($"Document '{typeof(TRow).Name}' {row.id} can not be updated because it is already changed");

            await _DeleteInternal(row.PartitionKey, row.id).ConfigureAwait(false);

            row.etag = Guid.NewGuid().ToString();
            await _InsertInternal(row).ConfigureAwait(false);
        }

        async Task IColumnTable<TRow>.Delete(string partitionKey, string id)
        {
            var original = await _FindInternal(partitionKey, id).ConfigureAwait(false);
            if (original == null)
                throw new Exception($"Row '{typeof(TRow).Name}' {id} can not be deleted because it is already removed.");

            var sql = $"DELETE FROM `{_store._projectId}.{_store._datasetId}.{_tableId}` WHERE partitionkey = @partitionKey AND id = @id";
            var parameters = new[]
            {
                new BigQueryParameter("partitionKey", BigQueryDbType.String, partitionKey),
                new BigQueryParameter("id", BigQueryDbType.String, id)
            };
            await _store._client.ExecuteQueryAsync(sql, parameters).ConfigureAwait(false);
        }

        async Task<TRow> IColumnTable<TRow>.Find(string partitionKey, string id)
        {
            var sql = $"SELECT partitionkey, id, etag FROM `{_store._projectId}.{_store._datasetId}.{_tableId}` WHERE partitionkey = @partitionKey AND id = @id LIMIT 1";
            var parameters = new[]
            {
                new BigQueryParameter("partitionKey", BigQueryDbType.String, partitionKey),
                new BigQueryParameter("id", BigQueryDbType.String, id)
            };

            var result = await _store._client.ExecuteQueryAsync(sql, parameters).ConfigureAwait(false);
            BigQueryRow bigQueryRow = result.FirstOrDefault();
            if (bigQueryRow == null)
                return default;

            var row = new TRow();
            var accessors = MetadataHelper.GetAccessors<TRow>();

            foreach (var field in bigQueryRow.Schema.Fields)
                MetadataHelper.SetMetadata(row, field.Name, bigQueryRow[field.Name], accessors);

            return row;
        }

        object IColumnTable<TRow>.GetUnderlyingImplementation() => _store._client;

        private async Task<Entity> _FindInternal(string partitionKey, string id)
        {
            var sql = $"SELECT partitionkey, id, etag, lastupdate FROM `{_store._projectId}.{_store._datasetId}.{_tableId}` WHERE partitionkey = @partitionKey AND id = @id LIMIT 1";
            var parameters = new[]
            {
                new BigQueryParameter("partitionKey", BigQueryDbType.String, partitionKey),
                new BigQueryParameter("id", BigQueryDbType.String, id)
            };

            var result = await _store._client.ExecuteQueryAsync(sql, parameters).ConfigureAwait(false);
            var row = result.FirstOrDefault();
            if (row == null)
                return default;

            return new Entity
            {
                PartitionKey = (string)row["partitionkey"],
                id = (string)row["id"],
                etag = (string)row["etag"],
            };
        }

        async Task _DeleteInternal(string partitionKey, string id)
        {
            var sql = $"DELETE FROM `{_store._projectId}.{_store._datasetId}.{_tableId}` WHERE partitionkey = @partitionKey AND id = @id";
            var parameters = new[]
            {
                new BigQueryParameter("partitionKey", BigQueryDbType.String, partitionKey),
                new BigQueryParameter("id", BigQueryDbType.String, id)
            };
            await _store._client.ExecuteQueryAsync(sql, parameters).ConfigureAwait(false);
        }

        async Task _InsertInternal(TRow row)
        {
            BigQueryInsertRow insertRow = new()
            {
                { "partitionkey", row.PartitionKey },
                { "id", row.id },
                { "etag", row.etag }
            };

            var metadata = MetadataHelper.GetMetadata(row);
            foreach (var field in metadata)
                insertRow.Add(field.Key, field.Value);

            await _store._client.InsertRowAsync(_store._datasetId, _tableId, insertRow).ConfigureAwait(false);
        }
    }
}