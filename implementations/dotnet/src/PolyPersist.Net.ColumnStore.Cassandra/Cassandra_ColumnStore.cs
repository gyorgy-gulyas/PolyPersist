using Cassandra;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    public class Cassandra_ColumnStore : IColumnStore
    {
        private readonly ISession _session;
        private readonly string _keyspace;

        public Cassandra_ColumnStore(string connectionString)
        {
            var config = CassandraConnectionStringParser.Parse(connectionString);
            var cluster = Cluster.Builder()
                .AddContactPoint(config.Host)
                .WithPort(config.Port)
                .WithCredentials(config.Username, config.Password)
                .Build();

            _session = cluster.Connect(config.Keyspace);
            _keyspace = config.Keyspace;
        }

        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.ColumnStore;
        string IStore.ProviderName => "Cassandra";

        async Task<bool> IColumnStore.IsTableExists(string tableName)
        {
            return await _IsTableExistsInternal( tableName).ConfigureAwait(false);
        }

        async Task<IColumnTable<TRow>> IColumnStore.GetTableByName<TRow>(string tableName)
        {
            if (await _IsTableExistsInternal(tableName).ConfigureAwait(false) == false )
                throw new Exception($"Table '{tableName}' does not exist");

            return new Cassandra_ColumnTable<TRow>(_session, tableName);
        }

        async Task<IColumnTable<TRow>> IColumnStore.CreateTable<TRow>(string tableName)
        {
            if (await _IsTableExistsInternal(tableName).ConfigureAwait(false) == true )
                throw new Exception($"Table '{tableName}' already exists in Cassandra keyspace '{_keyspace}'.");

            string createQuery = $@"
                CREATE TABLE {_keyspace}.{tableName} (
                    partitionkey text,
                    id text,
                    etag text,
                    PRIMARY KEY (partitionkey, id)
                )";
            await _session.ExecuteAsync(new SimpleStatement(createQuery));

            return new Cassandra_ColumnTable<TRow>(_session, tableName);
        }

        async Task IColumnStore.DropTable(string tableName)
        {
            if (await _IsTableExistsInternal(tableName).ConfigureAwait(false) == false )
                throw new Exception($"Table '{tableName}' does not exist in Cassandra keyspace '{_keyspace}'.");

            var dropQuery = $"DROP TABLE {_keyspace}.{tableName}";
            await _session.ExecuteAsync(new SimpleStatement(dropQuery));
        }

        private async Task<bool> _IsTableExistsInternal(string tableName)
        {
            var tables = await _session.ExecuteAsync(new SimpleStatement("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", _keyspace)).ConfigureAwait(false);
            return tables.Any(row => row.GetValue<string>("table_name") == tableName);
        }
    }

    public class CassandraConnectionInfo
    {
        public string Host { get; set; }
        public int Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Keyspace { get; set; }
    }

    public static class CassandraConnectionStringParser
    {
        public static CassandraConnectionInfo Parse(string connectionString)
        {
            var result = new CassandraConnectionInfo();
            var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);

            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in parts)
            {
                var kv = part.Split('=', 2);
                if (kv.Length == 2)
                {
                    dict[kv[0].Trim()] = kv[1].Trim();
                }
            }

            dict.TryGetValue("host", out var host);
            dict.TryGetValue("port", out var port);
            dict.TryGetValue("username", out var username);
            dict.TryGetValue("password", out var password);
            dict.TryGetValue("keyspace", out var keyspace);

            return new CassandraConnectionInfo
            {
                Host = host,
                Port = int.TryParse(port, out var p) ? p : 9042,
                Username = username,
                Password = password,
                Keyspace = keyspace,
            };
        }
    }
}
