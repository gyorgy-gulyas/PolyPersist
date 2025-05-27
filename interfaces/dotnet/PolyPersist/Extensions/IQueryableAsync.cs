namespace PolyPersist.Net.Extensions
{
    public interface IQueryableAsync<T> : IQueryable<T>, IAsyncEnumerable<T>
    {
    }

    public static class AsyncQueryableCaster
    {
        public static IQueryableAsync<T> AsAsync<T>(this IQueryable<T> source)
        {
            return source as IQueryableAsync<T>
                ?? throw new InvalidCastException("Source is not IQueryableAsync");
        }
    }
}
