namespace PolyPersist.Net.ColumnStore.Memory
{
    public class Memory_ColumnStore : IColumnStore
    {
        internal List<_TableData> _Tables = [];

        public Memory_ColumnStore(string connectionString )
        {
        }

        /// <inheritdoc/>
        IStore.StorageModels IStore.StorageModel => IStore.StorageModels.ColumnStore;
        /// <inheritdoc/>
        string IStore.ProviderName => "Memory_ColumnStore";

        /// <inheritdoc/>
        Task<bool> IColumnStore.IsTableExists(string tableName)
        {
            if (_Tables.FindIndex(c => c.Name == tableName) != -1)
                return Task.FromResult(true);
            else
                return Task.FromResult(false);
        }

        /// <inheritdoc/>
        Task<IColumnTable<TRow>> IColumnStore.GetTableByName<TRow>(string tableName)
        {
            _TableData tableData = _Tables.Find(c => c.Name == tableName);
            if (tableData == null)
                throw new Exception($"Table '{tableName}' does not exist in Memory ColumnStore Store");

            IColumnTable<TRow> table = new Memory_ColumnTable<TRow>( tableName, tableData, this);
            return Task.FromResult(table);
        }

        /// <inheritdoc/>
        Task<IColumnTable<TRow>> IColumnStore.CreateTable<TRow>(string tableName)
        {
            if (_Tables.FindIndex(c => c.Name == tableName) != -1)
                throw new Exception($"Table '{tableName}' is already exist");

            _TableData tableData = new(tableName);
            _Tables.Add(tableData);

            IColumnTable<TRow> table = new Memory_ColumnTable<TRow>(tableName, tableData, this);
            return Task.FromResult(table);
        }

        /// <inheritdoc/>
        Task IColumnStore.DropTable(string tableName)
        {
            _TableData tableData = _Tables.Find(c => c.Name == tableName);
            if (tableData == null)
                throw new Exception($"Table '{tableName}' does not exist in Memory ColumnStore");

            _Tables.Remove(tableData);
            return Task.CompletedTask;
        }

    }

    public class _TableData
    {
        internal _TableData(string name)
        {
            Name = name;
        }

        internal string Name;
        internal Dictionary<string, _RowData> MapOfDocments = [];
        internal List<_RowData> ListOfDocments = [];
        internal List<(string name, string[] keys)> Indexes = [];
    }

    public class _RowData
    {
        internal string id;
        internal string partitionKey;
        internal string etag;
        internal string Value;
    }
}
