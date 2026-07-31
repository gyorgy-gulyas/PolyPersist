namespace PolyPersist.Net.Core
{
    /// <summary>
    /// Concrete, instantiable <see cref="ICacheEntry{T}"/> for cache stores to return from TryGet.
    /// The counterpart of <see cref="Entity"/> for <see cref="IEntity"/>.
    /// <para>
    /// It is immutable and built through <see cref="Hit"/> / <see cref="Miss"/> rather than an object
    /// initialiser, so a store cannot accidentally report a value with Found still false (or the
    /// reverse). A miss carries no value at all, so one instance per T serves every miss.
    /// </para>
    /// </summary>
    public sealed class CacheEntry<T> : ICacheEntry<T>
    {
        /// <inheritdoc/>
        public bool Found { get; }
        /// <inheritdoc/>
        public T Value { get; }

        private CacheEntry(bool found, T value)
        {
            Found = found;
            Value = value;
        }

        /// <summary>The key was present and live; <paramref name="value"/> is what it held.</summary>
        public static ICacheEntry<T> Hit(T value) => new CacheEntry<T>(true, value);

        /// <summary>The key was absent or had expired.</summary>
        public static ICacheEntry<T> Miss { get; } = new CacheEntry<T>(false, default!);
    }
}
