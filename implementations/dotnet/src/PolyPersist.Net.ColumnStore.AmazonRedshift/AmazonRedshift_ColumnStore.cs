using Amazon.RedshiftDataAPIService;
using Amazon.RedshiftDataAPIService.Model;
using System.Data;

namespace PolyPersist.Net.ColumnStore.AmazonRedshift
{
    internal class AmazonRedshift_ColumnStore : IColumnStore
    {
        private readonly AmazonRedshiftDataAPIServiceClient _client;
        private readonly string _database;
        private readonly string _clusterId;
        private readonly string _dbUser;

        public AmazonRedshift_ColumnStore(string connectionString)
        {
            var config = RedshiftConnectionStringParser.Parse(connectionString);
            _client = new AmazonRedshiftDataAPIServiceClient();
            _database = config.Database;
            _clusterId = config.ClusterId;
            _dbUser = config.DbUser;
        }

        public IStore.StorageModels StorageModel => IStore.StorageModels.ColumnStore;
        public string ProviderName => "AmazonRedshift";
        public string Name => _database;

        public async Task<bool> IsTableExists(string tableName)
        {
            var sql = $"SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{tableName}'";
            var exec = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                ClusterIdentifier = _clusterId,
                Database = _database,
                DbUser = _dbUser,
                Sql = sql,
                WithEvent = true
            });

            string statementId = exec.Id;
            var result = await _client.GetStatementResultAsync(new GetStatementResultRequest
            {
                Id = statementId
            });

            return result.Records.Count > 0;
        }

        public async Task<IColumnTable<TRow>> CreateTable<TRow>(string tableName) where TRow : IRow, new()
        {
            if (await IsTableExists(tableName))
                throw new Exception($"Table '{tableName}' already exists in Redshift.");

            var sql = $"CREATE TABLE \"{tableName}\" (partitionkey VARCHAR(256), id VARCHAR(256), etag VARCHAR(256), PRIMARY KEY(partitionkey, id))";
            await ExecuteQuery(sql);

            return new AmazonRedshift_ColumnTable<TRow>(tableName, _client, _database, _clusterId, _dbUser);
        }

        public async Task<IColumnTable<TRow>> GetTableByName<TRow>(string tableName) where TRow : IRow, new()
        {
            if (!await IsTableExists(tableName)) return null;
            return new AmazonRedshift_ColumnTable<TRow>(tableName, _client, _database, _clusterId, _dbUser);
        }

        public async Task DropTable(string tableName)
        {
            if (!await IsTableExists(tableName))
                throw new Exception($"Table '{tableName}' does not exist in Redshift.");

            var sql = $"DROP TABLE \"{tableName}\"";
            await ExecuteQuery(sql);
        }

        private async Task ExecuteQuery(string sql)
        {
            await _client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                ClusterIdentifier = _clusterId,
                Database = _database,
                DbUser = _dbUser,
                Sql = sql
            });
        }
    }

    public class RedshiftConnectionInfo
    {
        public string ClusterId { get; set; }
        public string Database { get; set; }
        public string DbUser { get; set; }
    }

    public static class RedshiftConnectionStringParser
    {
        public static RedshiftConnectionInfo Parse(string connectionString)
        {
            var dict = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Split('=')).ToDictionary(k => k[0].Trim(), v => v[1].Trim(), StringComparer.OrdinalIgnoreCase);

            return new RedshiftConnectionInfo
            {
                ClusterId = dict.TryGetValue("clusterid", out var cid) ? cid : null,
                Database = dict.TryGetValue("database", out var db) ? db : null,
                DbUser = dict.TryGetValue("dbuser", out var user) ? user : null
            };
        }
    }
}

