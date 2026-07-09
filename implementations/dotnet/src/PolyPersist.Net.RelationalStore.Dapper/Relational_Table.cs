using System.Data.Common;
using System.Reflection;
using Dapper;
using LinqToDB;
using LinqToDB.Data;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.RelationalStore.Dapper
{
    /// <summary>
    /// A single relational table. Portable CRUD (Insert/Update/Delete/Find) + single-table LINQ
    /// query. Optimistic concurrency is enforced via the entity's <c>etag</c>. Joins are not here
    /// (not portable) - use <see cref="Relational_Store"/>.Query(). The <c>class</c> constraint is
    /// required by linq2db; the store bridges to it via reflection.
    /// </summary>
    internal class Relational_Table<TRecord> : ITable<TRecord>, IRelationalTableInternal
        where TRecord : class, IRecord, new()
    {
        // Public read/write properties of the record -> columns (lower-cased to match the mapping).
        private static readonly PropertyInfo[] _props = typeof(TRecord)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .ToArray();

        private readonly string _name;
        private readonly Relational_Store _store;

        // Public so the store can build it via Activator.CreateInstance across the class-constraint gap.
        public Relational_Table(string name, Relational_Store store)
        {
            _name = name;
            _store = store;
        }

        Task IRelationalTableInternal.CreateSchemaAsync() => _CreateSchemaAsync();

        private async Task _CreateSchemaAsync()
        {
            using var db = _store.CreateConnection();
            await db.CreateTableAsync<TRecord>(tableName: _name).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        string ITable<TRecord>.Name => _name;
        /// <inheritdoc/>
        IStore ITable<TRecord>.ParentStore => _store;

        /// <inheritdoc/>
        async Task ITable<TRecord>.Insert(TRecord record)
        {
            CollectionCommon.CheckBeforeInsert(record);

            if (string.IsNullOrEmpty(record.id) == true)
                record.id = Guid.NewGuid().ToString();

            record.etag = Guid.NewGuid().ToString();
            record.LastUpdate = DateTime.UtcNow;

            using var db = _store.CreateConnection();
            await db.InsertAsync(record, tableName: _name).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task ITable<TRecord>.Update(TRecord record)
        {
            CollectionCommon.CheckBeforeUpdate(record);

            using var db = _store.CreateConnection();

            // Read-check first for clear error messages (does-not-exist vs already-changed).
            TRecord? stored = await db.GetTable<TRecord>().TableName(_name)
                .FirstOrDefaultAsync(r => r.id == record.id && r.PartitionKey == record.PartitionKey)
                .ConfigureAwait(false);
            CollectionCommon.CheckEtagMatch(stored, record);

            record.etag = Guid.NewGuid().ToString();
            record.LastUpdate = DateTime.UtcNow;

            // The actual write is a Dapper UPDATE. Identifiers are double-quoted (valid on both
            // SQLite and PostgreSQL) so the property-cased column names match what linq2db created;
            // this avoids linq2db's entity-update PK handling.
            string setClause = string.Join(", ",
                _props.Where(p => _IsId(p) == false).Select(p => $"\"{p.Name}\" = @{p.Name}"));
            string sql = $"UPDATE \"{_name}\" SET {setClause} WHERE \"id\" = @id AND \"PartitionKey\" = @PartitionKey";

            var parameters = new DynamicParameters();
            foreach (var p in _props)
                parameters.Add(p.Name, p.GetValue(record));

            int affected = await ((DbConnection)db.Connection).ExecuteAsync(sql, parameters).ConfigureAwait(false);
            if (affected == 0)
                throw new Exception($"Record '{typeof(TRecord).Name}' {record.id} can not be updated because it is already changed");
        }

        private static bool _IsId(PropertyInfo p) => string.Equals(p.Name, nameof(IEntity.id), StringComparison.Ordinal);

        /// <inheritdoc/>
        async Task ITable<TRecord>.Delete(string partitionKey, string id)
        {
            using var db = _store.CreateConnection();

            int affected = await db.GetTable<TRecord>().TableName(_name)
                .Where(r => r.id == id && r.PartitionKey == partitionKey)
                .DeleteAsync()
                .ConfigureAwait(false);

            if (affected == 0)
                throw new Exception($"Record '{typeof(TRecord).Name}' {id} can not be removed because it is already removed");
        }

        /// <inheritdoc/>
        async Task<TRecord> ITable<TRecord>.Find(string partitionKey, string id)
        {
            using var db = _store.CreateConnection();

            // (partitionKey, id) identifies the row: a matching id in another partition is not it.
            return (await db.GetTable<TRecord>().TableName(_name)
                .FirstOrDefaultAsync(r => r.id == id && r.PartitionKey == partitionKey)
                .ConfigureAwait(false))!;
        }

        /// <inheritdoc/>
        System.Linq.IQueryable<TRecord> ITable<TRecord>.Query()
        {
            // A DataContext (not DataConnection) backs the returned IQueryable: it opens/closes the
            // ADO connection per query, so the queryable can be composed and enumerated after this
            // method returns without holding a connection open.
            var ctx = new DataContext(_store.Options());
            return ctx.GetTable<TRecord>().TableName(_name);
        }

        /// <inheritdoc/>
        object ITable<TRecord>.GetUnderlyingImplementation()
        {
            // A fresh linq2db DataConnection for raw SQL (e.g. Dapper) or a native transaction.
            // The caller owns and disposes it.
            return _store.CreateConnection();
        }
    }
}
