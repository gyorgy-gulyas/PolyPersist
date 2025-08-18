using System.Text.Json.Serialization;
using System.Text.Json;

namespace PolyPersist.Net.Common
{
    public static class JsonOptionsProvider
    {
        public static JsonSerializerOptions Options()
        {
            var options = new JsonSerializerOptions()
            {
                WriteIndented = true,
                Converters = { new JsonStringEnumConverter() }
            };

            PolymorphismHandler.Configure(options);

            return options;
        }
    }
}
