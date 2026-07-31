using System.Text.Json;

namespace PolyPersist.Net.Common
{
    /// <summary>
    /// Serializes the arbitrary values an <see cref="ICacheStore"/> holds. Centralised so every cache
    /// backend encodes a value identically: a value written through the in-process cache and one
    /// written through a RESP server round-trip the same way.
    /// </summary>
    public static class CacheValue
    {
        /// <summary>The JSON literal a null value is stored as.</summary>
        private const string _Null = "null";

        // Same converters as JsonOptionsProvider (enum-as-string, polymorphism) but NOT indented:
        // a cache value travels over the wire on every read and write, so the padding is pure waste.
        private static readonly JsonSerializerOptions _options = _BuildOptions();

        private static JsonSerializerOptions _BuildOptions()
        {
            var options = JsonOptionsProvider.Options();
            options.WriteIndented = false;
            return options;
        }

        /// <summary>
        /// Serializes against the STATIC type T, so a polymorphic value cached as its base type keeps
        /// its type discriminator (mirrors <see cref="BlobMetadata"/>).
        /// </summary>
        public static string Serialize<T>(T value)
            => JsonSerializer.Serialize(value, typeof(T), _options);

        /// <summary>
        /// Reads a cached value. A stored null yields default(T) rather than throwing, which is what a
        /// caller asking for a value type (e.g. <c>Get&lt;int&gt;</c>) of a null entry must get.
        /// </summary>
        public static T Deserialize<T>(string json)
            => json == _Null ? default! : JsonSerializer.Deserialize<T>(json, _options)!;
    }
}
