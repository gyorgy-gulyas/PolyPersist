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

        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.ColumnStore;
        string IStore.ProviderName => "AmazonRedshift";

        async Task<bool> IColumnStore.IsTableExists(string tableName)
        {
            return await _IsTableExistsInternal( tableName ).ConfigureAwait(false);
        }

        async Task<IColumnTable<TRow>> IColumnStore.CreateTable<TRow>(string tableName)
        {
            if (await _IsTableExistsInternal(tableName).ConfigureAwait(false) == true )
                throw new Exception($"Table '{tableName}' already exists in Redshift.");

            var sql = $"CREATE TABLE \"{tableName}\" (partitionkey VARCHAR(256), id VARCHAR(256), etag VARCHAR(256), PRIMARY KEY(partitionkey, id))";
            await _ExecuteQueryInternal(sql);

            return new AmazonRedshift_ColumnTable<TRow>(tableName, _client, _database, _clusterId, _dbUser);
        }

        async Task<IColumnTable<TRow>> IColumnStore.GetTableByName<TRow>(string tableName)
        {
            if (await _IsTableExistsInternal(tableName).ConfigureAwait(false) == false)
                return null;

            return new AmazonRedshift_ColumnTable<TRow>(tableName, _client, _database, _clusterId, _dbUser);
        }

        async Task IColumnStore.DropTable(string tableName)
        {
            if (await _IsTableExistsInternal(tableName).ConfigureAwait(false) == false)
                throw new Exception($"Table '{tableName}' does not exist in Redshift.");

            var sql = $"DROP TABLE \"{tableName}\"";
            await _ExecuteQueryInternal(sql);
        }

        private async Task<ExecuteStatementResponse> _ExecuteQueryInternal(string sql)
        {
            return await _client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                ClusterIdentifier = _clusterId,
                Database = _database,
                DbUser = _dbUser,
                Sql = sql
            }).ConfigureAwait(false);
        }

        async Task<bool> _IsTableExistsInternal(string tableName)
        {
            var sql = $"SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{tableName}'";
            var exec = await _ExecuteQueryInternal( sql).ConfigureAwait(false);

            var result = await _client.GetStatementResultAsync(new GetStatementResultRequest
            {
                Id = exec.Id
            }).ConfigureAwait(false);

            return result.Records.Count > 0;
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

