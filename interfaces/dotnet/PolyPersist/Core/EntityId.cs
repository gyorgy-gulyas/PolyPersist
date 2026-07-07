#nullable enable
using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PolyPersist.Net.Core
{
    /// <summary>
    /// Strongly-typed entity identifier. <c>EntityId&lt;Customer&gt;</c> and
    /// <c>EntityId&lt;Order&gt;</c> are distinct types at compile time, so ids can no
    /// longer be mixed up, while the underlying value stays a plain <see cref="string"/>
    /// (matching <c>IEntity.id : string</c>). Implicit string conversions keep the
    /// PolyPersist boundary (which takes <c>string id</c>) friction-free.
    ///
    /// TODO: constrain to <c>where TEntity : IEntity</c> once the d3i-generated
    /// aggregate roots implement <c>IEntity</c> (see the PolyPersist adaptation todo).
    /// </summary>
    [JsonConverter(typeof(EntityIdJsonConverterFactory))]
    public readonly record struct EntityId<TEntity>(string Value) : IParsable<EntityId<TEntity>>
    {
        public override string ToString() => Value;

        // Automatic string <-> EntityId conversion (domain code + PolyPersist string apis).
        public static implicit operator string(EntityId<TEntity> id) => id.Value;
        public static implicit operator EntityId<TEntity>(string value) => new(value);

        // string parsing for REST route/query binding, config, etc.
        public static EntityId<TEntity> Parse(string s, IFormatProvider? provider) => new(s);

        public static bool TryParse(string? s, IFormatProvider? provider, out EntityId<TEntity> result)
        {
            result = new(s ?? string.Empty);
            return s is not null;
        }
    }

    /// <summary>
    /// A single JSON converter for every <c>EntityId&lt;&gt;</c> (open generic): each is
    /// serialized as a plain string on the wire, not as an object. Attached via the
    /// <c>[JsonConverter]</c> attribute on <see cref="EntityId{TEntity}"/>, so no per-type
    /// converter and no global registration are needed.
    /// </summary>
    public sealed class EntityIdJsonConverterFactory : JsonConverterFactory
    {
        public override bool CanConvert(Type typeToConvert) =>
            typeToConvert.IsGenericType &&
            typeToConvert.GetGenericTypeDefinition() == typeof(EntityId<>);

        public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options)
        {
            Type entityType = type.GetGenericArguments()[0];
            Type converterType = typeof(EntityIdJsonConverter<>).MakeGenericType(entityType);
            return (JsonConverter)Activator.CreateInstance(converterType)!;
        }

        private sealed class EntityIdJsonConverter<TEntity> : JsonConverter<EntityId<TEntity>>
        {
            public override EntityId<TEntity> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
                => new(reader.GetString() ?? string.Empty);

            public override void Write(Utf8JsonWriter writer, EntityId<TEntity> value, JsonSerializerOptions options)
                => writer.WriteStringValue(value.Value);
        }
    }
}
