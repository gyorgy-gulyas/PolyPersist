using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;

namespace PolyPersist.Net.MongoDB
{
    public static class MongoDB_Serializer
    {
        private static readonly object locker = new object();

        public static JsonWriterSettings WriterSettings;

        static MongoDB_Serializer()
        {
            WriterSettings = new JsonWriterSettings { OutputMode = JsonOutputMode.CanonicalExtendedJson };

            ConventionRegistry.Register(nameof(EnumRepresentationConvention)
                , new ConventionPack { new EnumRepresentationConvention(BsonType.String) }
                , t => true);
        }

        static public void RegisterType<T>(Type type)
        {
            lock (locker)
            {
                if (BsonClassMap.IsClassMapRegistered(type) == false)
                {
                    BsonClassMap.RegisterClassMap<T>(classMap =>
                    {
                        classMap.AutoMap();
                    });
                }
            }
        }
    }
}
