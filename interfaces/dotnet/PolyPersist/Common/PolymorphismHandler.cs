using System.Collections;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace PolyPersist.Net.Common
{
    /// <summary>
    /// Polimorf deszerializáció központi, attribútummentes konfigurátora .NET 8-hoz.
    /// Használat:
    /// PolymorphismHandler.Register<Animal, Dog>("dog");
    /// PolymorphismHandler.Register<Animal, Cat>("cat");
    /// PolymorphismHandler.Configure(options);
    /// 
    /// Alapértelmezett diszkriminátor mező: "$type".
    /// </summary>
    public static class PolymorphismHandler
    {
        private static readonly object _gate = new();
        // baseType -> list of (derivedType, discriminator)
        private static readonly Dictionary<Type, List<(Type Derived, object Discriminator)>> _map = new();

        private static string _typeDiscriminatorPropertyName = "$type";
        private static bool _ignoreUnrecognized = true;

        /// <summary>
        /// Ismeretlen diszkriminátor esetén dobjon-e kivételt, vagy hagyja figyelmen kívül (alap: true = ignorál).
        /// </summary>
        public static void SetIgnoreUnrecognized(bool ignore) => _ignoreUnrecognized = ignore;

        /// <summary>
        /// Leszármazott típus regisztrálása string diszkriminátorral.
        /// </summary>
        public static void Register<TBase, TDerived>()
            where TDerived : TBase
        {
            RegisterCore(typeof(TBase), typeof(TDerived), typeof(TDerived).Name);
        }

        public static IEnumerable<(Type Derived, object Discriminator)> GetDerivedTypes(Type baseType)
        {
            if (_map.TryGetValue(baseType, out List<(Type Derived, object Discriminator)> result) == true)
                return result;

            return [];
        }
            

        /// <summary>
        /// Az összegyűjtött regisztrációk alkalmazása a megadott JsonSerializerOptions példányra.
        /// Nem írja felül a meglévő resolver-t, hanem kombinálja vele.
        /// </summary>
        public static void Configure(JsonSerializerOptions options)
        {
            if (options is null) throw new ArgumentNullException(nameof(options));

            // Lemásoljuk a pillanatnyi állapotot a lock-ból kilépve használathoz.
            Dictionary<Type, List<(Type Derived, object Discriminator)>> snapshot;
            string propName; bool ignore;
            lock (_gate)
            {
                snapshot = new Dictionary<Type, List<(Type, object)>>(_map.Count);
                foreach (var kvp in _map)
                    snapshot[kvp.Key] = new List<(Type, object)>(kvp.Value);

                propName = _typeDiscriminatorPropertyName;
                ignore = _ignoreUnrecognized;
            }

            if (snapshot.Count == 0)
                return; // nincs mit konfigurálni

            var resolver = new DefaultJsonTypeInfoResolver
            {
                Modifiers =
                {
                    ti =>
                    {
                        if (!snapshot.TryGetValue(ti.Type, out var derivedList))
                            return;

                        var poly = new JsonPolymorphismOptions
                        {
                            TypeDiscriminatorPropertyName = propName,
                            IgnoreUnrecognizedTypeDiscriminators = ignore
                        };

                        foreach (var (derived, disc) in derivedList)
                        {
                            switch (disc)
                            {
                                case string s:
                                    poly.DerivedTypes.Add(new JsonDerivedType(derived, s));
                                    break;
                                case int i:
                                    poly.DerivedTypes.Add(new JsonDerivedType(derived, i));
                                    break;
                                default:
                                    throw new NotSupportedException(
                                        $"A diszkriminátor típusa nem támogatott: {disc.GetType().Name}. Csak string vagy int engedélyezett.");
                            }
                        }

                        ti.PolymorphismOptions = poly;
                    }
                }
            };

            options.TypeInfoResolver = options.TypeInfoResolver is null
                ? resolver
                : JsonTypeInfoResolver.Combine(options.TypeInfoResolver, resolver);
        }

        /// <summary>
        /// Összes regisztráció törlése (teszteléshez, újrakonfiguráláshoz).
        /// </summary>
        public static void Clear()
        {
            lock (_gate)
            {
                _map.Clear();
            }
        }

        // --- belső közös regisztráció ---
        private static void RegisterCore(Type baseType, Type derivedType, object discriminator)
        {
            if (baseType is null) throw new ArgumentNullException(nameof(baseType));
            if (derivedType is null) throw new ArgumentNullException(nameof(derivedType));

            if (!baseType.IsAssignableFrom(derivedType))
                throw new ArgumentException($"{derivedType} nem származik {baseType} típusból.");

            lock (_gate)
            {
                if (!_map.TryGetValue(baseType, out var list))
                {
                    list = new List<(Type, object)>();
                    _map[baseType] = list;
                }

                // Duplikátum ellenőrzés (vagy derived, vagy diszkriminátor ütközés)
                foreach (var (existingDerived, existingDisc) in list)
                {
                    if (existingDerived == derivedType)
                        throw new InvalidOperationException($"A {derivedType} már regisztrálva van {baseType} alá.");

                    if (Equals(existingDisc, discriminator))
                        throw new InvalidOperationException(
                            $"A(z) {baseType} alatt a(z) \"{discriminator}\" diszkriminátor már használatban van ({existingDerived}).");
                }

                list.Add((derivedType, discriminator));
            }
        }
    }
}
