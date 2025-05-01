namespace PolyPersist.Net.Extensions
{
    public static class ColumnTableExtensions
    {
        /// <summary>
        /// Provides a strongly-typed IQueryable interface for the given IColumnTable.
        /// Throws InvalidCastException if the implementation does not return IQueryable.
        /// </summary>
        public static IQueryable<TRow> AsQueryable<TRow>(this IColumnTable<TRow> table)
            where TRow : IRow, new()
        {
            if (table is null)
                throw new ArgumentNullException(nameof(table));

            var query = table.Query();

            if (query is IQueryable<TRow> typedQuery)
            {
                return typedQuery;
            }

            throw new InvalidCastException($"The returned query object from table '{table.Name}' is not an IQueryable<{typeof(TRow).Name}>.");
        }
    }
}
