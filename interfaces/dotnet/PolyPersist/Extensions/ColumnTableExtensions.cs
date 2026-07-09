namespace PolyPersist.Net.Extensions
{
    public static class ColumnTableExtensions
    {
        /// <summary>
        /// Strongly-typed CROSS-PARTITION queryable for the table (spans every partition).
        /// Throws InvalidCastException if the implementation does not return IQueryable.
        /// </summary>
        public static IQueryable<TRow> AsQueryable<TRow>(this IColumnTable<TRow> table)
            where TRow : IRow, new()
        {
            if (table is null)
                throw new ArgumentNullException(nameof(table));

            var query = table.QueryCrossPartition();

            if (query is IQueryable<TRow> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidCastException($"The returned query object from table '{table.Name}' is not an IQueryable<{typeof(TRow).Name}>.");
        }

        /// <summary>
        /// Strongly-typed queryable SCOPED to a single partition (already filtered by partitionKey).
        /// Throws InvalidCastException if the implementation does not return IQueryable.
        /// </summary>
        public static IQueryable<TRow> AsQueryable<TRow>(this IColumnTable<TRow> table, string partitionKey)
            where TRow : IRow, new()
        {
            if (table is null)
                throw new ArgumentNullException(nameof(table));

            var query = table.Query(partitionKey);

            if (query is IQueryable<TRow> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidCastException($"The returned query object from table '{table.Name}' is not an IQueryable<{typeof(TRow).Name}>.");
        }
    }
}
