using System.Text.Json;

namespace PolyPersist.Net.Common
{
    /// <summary>
    /// A blob's entity fields are stored as a JSON document alongside its content (in the object's
    /// metadata, a sidecar file, etc.) under <see cref="Key"/>. Centralising the key and the
    /// serializer options here means a read and a write can never drift apart, and every store
    /// serializes a blob identically (previously some used the default options and some used
    /// <see cref="JsonOptionsProvider"/>, so an option-sensitive field could round-trip differently
    /// per store).
    /// </summary>
    public static class BlobMetadata
    {
        /// <summary>The metadata key the serialized blob JSON is stored under.</summary>
        public const string Key = "meta_json";

        // Same converters as JsonOptionsProvider (enum-as-string, polymorphism) but NOT indented:
        // this JSON is stored in object-metadata headers (S3/Azure/GCS/MinIO), which must be a
        // single line — an indented document would break the metadata write.
        private static readonly JsonSerializerOptions _options = BuildOptions();

        private static JsonSerializerOptions BuildOptions()
        {
            var options = JsonOptionsProvider.Options();
            options.WriteIndented = false;
            return options;
        }

        public static string Serialize<TBlob>(TBlob blob)
            => JsonSerializer.Serialize(blob, typeof(TBlob), _options);

        public static TBlob Deserialize<TBlob>(string json)
            => JsonSerializer.Deserialize<TBlob>(json, _options)!;
    }
}
