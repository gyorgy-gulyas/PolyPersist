using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using PolyPersist.Net.Common;

namespace PolyPersist.Net.DocumentStore.MongoDB
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
            var derivedTypes = PolymorphismHandler.GetDerivedTypes(type);
            lock (locker)
            {
                if (BsonClassMap.IsClassMapRegistered(type) == false)
                {
                    BsonClassMap.RegisterClassMap<T>(classMap =>
                    {
                        classMap.AutoMap();
                        if(derivedTypes.Any())
                            classMap.SetIsRootClass(true);
                    });
                }

                foreach (var derivedType in derivedTypes)
                {
                    if (!BsonClassMap.IsClassMapRegistered(derivedType.Derived))
                    {
                        var cm = new BsonClassMap(derivedType.Derived);
                        cm.AutoMap();
                        BsonClassMap.RegisterClassMap(cm);
                    }
                }
            }
        }
    }
}
