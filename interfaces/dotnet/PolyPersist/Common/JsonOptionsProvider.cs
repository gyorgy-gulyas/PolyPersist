using System.Text.Json.Serialization;
using System.Text.Json;

namespace PolyPersist.Net.Common
{
    public static class JsonOptionsProvider
    {
        public static JsonSerializerOptions Options => new()
        {
            WriteIndented = true,
            Converters = { new JsonStringEnumConverter() }
        };
    }
}
