using Cassandra;
using Cassandra.Mapping;

namespace PolyPersist.Net.ColumnStore.Cassandra
{
    internal class Cassandra_ColumnTable<TRow> : IColumnTable<TRow> where TRow : IRow, new()
    {
        private readonly ISession _session;
        private readonly string _tableName;
        private readonly IMapper _mapper;

        public Cassandra_ColumnTable(ISession session, string tableName)
        {
            _session = session;
            _tableName = tableName;
            _mapper = new Mapper(session);
        }

        public string Name => _tableName;

        public async Task Insert(TRow row)
        {
            row.etag = Guid.NewGuid().ToString();
            await _mapper.InsertAsync(row);
        }

        public async Task Update(TRow row)
        {
            row.etag = Guid.NewGuid().ToString();
            await _mapper.UpdateAsync(row);
        }

        public async Task Delete(string partitionKey, string id)
        {
            var query = $"DELETE FROM {_tableName} WHERE partitionkey = ? AND id = ?";
            await _session.ExecuteAsync(new SimpleStatement(query, partitionKey, id));
        }

        public async Task<TRow> Find(string partitionKey, string id)
        {
            var query = $"SELECT * FROM {_tableName} WHERE partitionkey = ? AND id = ? LIMIT 1";
            var result = await _mapper.FetchAsync<TRow>(query, partitionKey, id);
            return result.FirstOrDefault();
        }

        public TQuery Query<TQuery>()
        {
            if (typeof(TQuery) != typeof(IQueryable<TRow>))
                throw new NotSupportedException("Only IQueryable<T> supported in CassandraColumnTable.");

            return (TQuery)(object)_mapper.Fetch<TRow>().AsQueryable();
        }

        public object GetUnderlyingImplementation() => _session;
    }
}