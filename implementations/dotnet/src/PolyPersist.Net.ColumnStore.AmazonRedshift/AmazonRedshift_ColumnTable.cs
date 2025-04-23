using Amazon.RedshiftDataAPIService;
using Amazon.RedshiftDataAPIService.Model;
using PolyPersist.Net.Common;
using PolyPersist.Net.Core;

namespace PolyPersist.Net.ColumnStore.AmazonRedshift
{
    internal class AmazonRedshift_ColumnTable<TRow> : IColumnTable<TRow> 
        where TRow : IRow, new()
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

        string IColumnTable<TRow>.Name => _tableName;

        async Task IColumnTable<TRow>.Insert(TRow row)
        {
            await CollectionCommon.CheckBeforeInsert(row).ConfigureAwait(false);
            row.etag = Guid.NewGuid().ToString();

            var metadata = MetadataHelper.GetMetadata(row);
            var columns = string.Join(", ", metadata.Keys.Select(fieldName => $"\"{fieldName}\""));
            var values = string.Join(", ", metadata.Values.Select(fieldValue => _escape(fieldValue)));

            var sql = $"INSERT INTO \"{_tableName}\" ({columns}) VALUES ({values})";

            await _ExecuteQueryInternal(sql).ConfigureAwait(false);
        }

        async Task IColumnTable<TRow>.Update(TRow row)
        {
            await CollectionCommon.CheckBeforeUpdate(row).ConfigureAwait(false);

            var original = await _FindInternal(row.PartitionKey, row.id).ConfigureAwait(false);
            if (original == null)
                throw new Exception($"Row '{typeof(TRow).Name}' {row.id} can not be updated because it is already removed.");

            if (row.etag != original.etag)
                throw new Exception($"Document '{typeof(TRow).Name}' {row.id} can not be updated because it is already changed");

            row.etag = Guid.NewGuid().ToString();

            var metadatas = MetadataHelper.GetMetadata(row);
            var setClauses = new List<string>();
            foreach (var metadata in metadatas)
                setClauses.Add($"\"{metadata.Key}\" = {_escape(metadata.Value)}");
            
            var sql = 
                $"UPDATE \"{_tableName} " + 
                $"   SET {string.Join(", ", setClauses)} " +
                $" WHERE partitionkey = '{row.PartitionKey} " + 
                $"   AND id = '{row.id}'";
            await _ExecuteQueryInternal(sql);
        }

        async Task IColumnTable<TRow>.Delete(string partitionKey, string id)
        {
            var query = $"DELETE FROM \"{_tableName}\" WHERE partitionkey = '{partitionKey}' AND id = '{id}'";
            await _ExecuteQueryInternal(query);
        }

        async Task<TRow> IColumnTable<TRow>.Find(string partitionKey, string id)
        {
            var query = $"SELECT * FROM \"{_tableName}\" WHERE partitionkey = '{partitionKey}' AND id = '{id}' LIMIT 1";

            var exec = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                ClusterIdentifier = _clusterId,
                Database = _database,
                DbUser = _dbUser,
                Sql = query
            }).ConfigureAwait(false);

            

            
            for (int i = 0; i < 5; i++)
            {
                var desc = await _client.DescribeStatementAsync(new DescribeStatementRequest { Id = exec.Id });
                
                if (desc.Status == StatusString.FINISHED)
                    break;

                if (desc.Status == StatusString.FAILED || desc.Status == StatusString.ABORTED)
                    throw new Exception($"Query failed: {desc.Error}");

                await Task.Delay(10);
            }

            var result = await _client.GetStatementResultAsync(new GetStatementResultRequest { Id = exec.Id });
            if (result.Records.Count == 0)
                return default;

            var row = new TRow();
            var record = result.Records[0];
            var fieldNames = result.ColumnMetadata.Select(cm => cm.Name).ToArray();
            var accessors = MetadataHelper.GetAccessors<TRow>();

            for (int i = 0; i < fieldNames.Length; i++)
                MetadataHelper.SetMetadata(record, fieldNames[i], record[i], accessors);

            return row;
        }

        public object GetUnderlyingImplementation() => _client;

        private async Task _ExecuteQueryInternal(string sql)
        {
            await _client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                ClusterIdentifier = _clusterId,
                Database = _database,
                DbUser = _dbUser,
                Sql = sql
            });
        }

        async Task<Entity> _FindInternal(string partitionKey, string id)
        {
            var query = $"SELECT partitionkey, id, etag FROM \"{_tableName}\" WHERE partitionkey = '{partitionKey}' AND id = '{id}' LIMIT 1";

            var exec = await _client.ExecuteStatementAsync(new ExecuteStatementRequest
            {
                ClusterIdentifier = _clusterId,
                Database = _database,
                DbUser = _dbUser,
                Sql = query,
            }).ConfigureAwait( false );

            int attempts = 0;
            while (attempts < 10)
            {
                var desc = await _client.DescribeStatementAsync(new DescribeStatementRequest
                {
                    Id = exec.Id
                });

                if (desc.Status == StatusString.FINISHED)
                    break;

                if (desc.Status == StatusString.FAILED || desc.Status == StatusString.ABORTED)
                    throw new Exception($"Redshift query failed with status: {desc.Status}, Error: {desc.Error}");

                await Task.Delay(10);
                attempts++;

            }

            var result = await _client.GetStatementResultAsync(new GetStatementResultRequest
            {
                Id = exec.Id
            });

            if (result.Records.Count == 0)
                return default;

            var record = result.Records[0];
            return new Entity
            {
                PartitionKey = record[0].StringValue,
                id = record[1].StringValue,
                etag = record[2].StringValue
            };
        }

        private static string _escape(object value)
        {
            if (value == null) return "NULL";
            return value.ToString();
        }
    }
}