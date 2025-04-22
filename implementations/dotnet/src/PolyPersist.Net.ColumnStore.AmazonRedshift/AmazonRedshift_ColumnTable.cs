using Amazon.RedshiftDataAPIService;
using Amazon.RedshiftDataAPIService.Model;

namespace PolyPersist.Net.ColumnStore.AmazonRedshift
{
    internal class AmazonRedshift_ColumnTable<TRow> : IColumnTable<TRow> where TRow : IRow, new()
    {
        private readonly string _tableName;
        private readonly AmazonRedshiftDataAPIServiceClient _client;
        private readonly string _database;
        private readonly string _clusterId;
        private readonly string _dbUser;

        public AmazonRedshift_ColumnTable(string tableName, AmazonRedshiftDataAPIServiceClient client, string database, string clusterId, string dbUser)
        {
            _tableName = tableName;
            _client = client;
            _database = database;
            _clusterId = clusterId;
            _dbUser = dbUser;
        }

        public string Name => _tableName;

        public async Task Insert(TRow row)
        {
            row.etag = Guid.NewGuid().ToString();
            var query = $"INSERT INTO \"{_tableName}\" (partitionkey, id, etag) VALUES ('{row.PartitionKey}', '{row.id}', '{row.etag}')";
            await ExecuteQuery(query);
        }

        public async Task Update(TRow row)
        {
            row.etag = Guid.NewGuid().ToString();
            var query = $"UPDATE \"{_tableName}\" SET etag = '{row.etag}' WHERE partitionkey = '{row.PartitionKey}' AND id = '{row.id}'";
            await ExecuteQuery(query);
        }

        public async Task Delete(string partitionKey, string id)
        {
            var query = $"DELETE FROM \"{_tableName}\" WHERE partitionkey = '{partitionKey}' AND id = '{id}'";
            await ExecuteQuery(query);
        }

        public async Task<TRow> Find(string partitionKey, string id)
        {
            var query = $"SELECT partitionkey, id, etag FROM \"{_tableName}\" WHERE partitionkey = '{partitionKey}' AND id = '{id}' LIMIT 1";

            var exec = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                ClusterIdentifier = _clusterId,
                Database = _database,
                DbUser = _dbUser,
                Sql = query,
                WithEvent = true
            });

            string statementId = exec.Id;
            GetStatementResultResponse result;
            do
            {
                await Task.Delay(100); // Polling delay
                result = await _client.GetStatementResultAsync(new GetStatementResultRequest
                {
                    Id = statementId
                });
            } while (result.Records.Count == 0 && result.TotalNumRows == 0);

            if (result.Records.Count == 0)
                return default;

            var record = result.Records[0];
            return new TRow
            {
                PartitionKey = record[0].StringValue,
                id = record[1].StringValue,
                etag = record[2].StringValue
            };
        }

        public TQuery Query<TQuery>()
        {
            throw new NotSupportedException("Amazon Redshift does not support IQueryable style queries via Data API.");
        }

        public object GetUnderlyingImplementation() => _client;

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
}