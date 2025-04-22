using Google.Cloud.BigQuery.V2;

namespace PolyPersist.Net.ColumnStore.GoogleBigQuery
{

    internal class GoogleBigQuery_ColumnStore : IColumnStore
    {
        internal readonly BigQueryClient _client;
        internal readonly string _datasetId;
        internal readonly string _projectId;

        public GoogleBigQuery_ColumnStore(string connectionString)
        {
            var config = BigQueryConnectionStringParser.Parse(connectionString);
            _datasetId = config.DatasetId;
            _projectId = config.ProjectId;
            _client = string.IsNullOrEmpty(config.CredentialPath)
                ? BigQueryClient.Create(_projectId)
                : BigQueryClient.Create(_projectId, Google.Apis.Auth.OAuth2.GoogleCredential.FromFile(config.CredentialPath));
        }

        public IStore.StorageModels StorageModel => IStore.StorageModels.ColumnStore;
        public string ProviderName => "GoogleBigQuery";
        public string Name => _datasetId;

        public async Task<bool> IsTableExists(string tableName)
        {
            var table = await _client.GetTableAsync(_datasetId, tableName);
            return table != null;
        }

        public async Task<IColumnTable<TRow>> CreateTable<TRow>(string tableName) where TRow : IRow, new()
        {
            if (await IsTableExists(tableName))
                throw new Exception($"Table '{tableName}' already exists in BigQuery dataset '{_datasetId}'.");

            var schema = new TableSchemaBuilder
            {
                { "partitionkey", BigQueryDbType.String },
                { "id", BigQueryDbType.String },
                { "etag", BigQueryDbType.String }
            }.Build();

            await _client.CreateTableAsync(_datasetId, tableName, schema);
            return new GoogleBigQuery_ColumnTable<TRow>(this, tableName);
        }

        public async Task<IColumnTable<TRow>> GetTableByName<TRow>(string tableName) where TRow : IRow, new()
        {
            if (!await IsTableExists(tableName))
                return null;

            return new GoogleBigQuery_ColumnTable<TRow>(this, tableName);
        }

        public async Task DropTable(string tableName)
        {
            if (!await IsTableExists(tableName))
                throw new Exception($"Table '{tableName}' does not exist in BigQuery dataset '{_datasetId}'.");

            await _client.DeleteTableAsync(_datasetId, tableName);
        }
    }

    public class BigQueryConnectionInfo
    {
        public string ProjectId { get; set; }
        public string DatasetId { get; set; }
        public string CredentialPath { get; set; }
    }

    public static class BigQueryConnectionStringParser
    {
        public static BigQueryConnectionInfo Parse(string connectionString)
        {
            var dict = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Split('=')).ToDictionary(k => k[0].Trim(), v => v[1].Trim(), StringComparer.OrdinalIgnoreCase);

            return new BigQueryConnectionInfo
            {
                ProjectId = dict.TryGetValue("projectid", out var p) ? p : null,
                DatasetId = dict.TryGetValue("datasetid", out var d) ? d : null,
                CredentialPath = dict.TryGetValue("credentialpath", out var c) ? c : null
            };
        }
    }
}

