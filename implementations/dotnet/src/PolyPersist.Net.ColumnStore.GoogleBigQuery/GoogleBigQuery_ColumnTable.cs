using Google.Cloud.BigQuery.V2;

namespace PolyPersist.Net.ColumnStore.GoogleBigQuery
{
    internal class GoogleBigQuery_ColumnTable<TRow> : IColumnTable<TRow> where TRow : IRow, new()
    {
        private readonly GoogleBigQuery_ColumnStore _store;
        private readonly string _tableId;

        public GoogleBigQuery_ColumnTable(GoogleBigQuery_ColumnStore store, string tableId)
        {
            _store = store;
            _tableId = tableId;
        }

        public string Name => _tableId;

        public async Task Insert(TRow row)
        {
            row.etag = Guid.NewGuid().ToString();
            var insertRow = new BigQueryInsertRow
            {
                { "partitionkey", row.PartitionKey },
                { "id", row.id },
                { "etag", row.etag }
            };
            await _store._client.InsertRowAsync(_store._datasetId, _tableId, insertRow);
        }

        public async Task Update(TRow row)
        {
            await Delete(row.PartitionKey, row.id);
            await Insert(row);
        }

        public async Task Delete(string partitionKey, string id)
        {
            var sql = $"DELETE FROM `{_store._projectId}.{_store._datasetId}.{_tableId}` WHERE partitionkey = @partitionKey AND id = @id";
            var parameters = new[]
            {
                new BigQueryParameter("partitionKey", BigQueryDbType.String, partitionKey),
                new BigQueryParameter("id", BigQueryDbType.String, id)
            };
            await _store._client.ExecuteQueryAsync(sql, parameters);
        }

        public async Task<TRow> Find(string partitionKey, string id)
        {
            var sql = $"SELECT partitionkey, id, etag FROM `{_store._projectId}.{_store._datasetId}.{_tableId}` WHERE partitionkey = @partitionKey AND id = @id LIMIT 1";
            var parameters = new[]
            {
                new BigQueryParameter("partitionKey", BigQueryDbType.String, partitionKey),
                new BigQueryParameter("id", BigQueryDbType.String, id)
            };

            var result = await _store._client.ExecuteQueryAsync(sql, parameters);
            var row = result.FirstOrDefault();
            if (row == null)
                return default;

            return new TRow
            {
                PartitionKey = (string)row["partitionkey"],
                id = (string)row["id"],
                etag = (string)row["etag"]
            };
        }

        public TQuery Query<TQuery>()
        {
            throw new NotSupportedException("BigQuery does not support IQueryable style queries in this context.");
        }

        public object GetUnderlyingImplementation() => _store._client;
    }
}