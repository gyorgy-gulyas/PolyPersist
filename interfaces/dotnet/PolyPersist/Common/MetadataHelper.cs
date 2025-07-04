﻿using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace PolyPersist.Net.Common
{
    public static class MetadataHelper
    {
        // Static cache to store property and field access delegates
        public class MemberAccessorContainer
        {
            internal Dictionary<string, MemberAccessor> NormalNames;
            internal Dictionary<string, MemberAccessor> LowerCaseNames;
        }

        private static readonly ConcurrentDictionary<Type, MemberAccessorContainer> _cache = new();

        /// Retrieves metadata from a given object, including its public properties and fields, as string values.
        public static IDictionary<string, string> GetMetadata<T>(T entity)
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity), "The entity cannot be null.");
            }

            var metadata = new Dictionary<string, string>();

            // Get cached accessors or generate new ones
            var accessors = _cache.GetOrAdd(typeof(T), GenerateAccessors);

            // Execute accessors to extract metadata
            foreach (var accessor in accessors.NormalNames.Values)
            {
                var value = accessor.Getter(entity);
                metadata[accessor.Name] = value switch
                {
                    null => string.Empty,
                    DateTime datetime => datetime.ToString("o"),
                    DateOnly dateonly => dateonly.ToString(CultureInfo.InvariantCulture),
                    TimeOnly timeonly => timeonly.ToString(CultureInfo.InvariantCulture),
                    decimal _decimal => _decimal.ToString(CultureInfo.InvariantCulture),
                    _ => value.ToString(),
                };
            }

            return metadata;
        }

        /// Populates a new instance of the specified type using metadata from a dictionary.
        public static T SetMetadata<T>(T entity, IDictionary<string, string> metadata) where T : new()
        {
            if (metadata == null)
            {
                throw new ArgumentNullException(nameof(metadata), "Metadata cannot be null.");
            }

            // Get cached accessors or generate new ones
            var accessors = _cache.GetOrAdd(typeof(T), GenerateAccessors);

            // Set values from metadata
            foreach (var accessor in accessors.NormalNames.Values)
            {
                if (metadata.TryGetValue(accessor.Name, out var value) && accessor.Setter != null)
                {
                    var convertedValue = ConvertToType(value, accessor.Type);
                    accessor.Setter(entity, convertedValue);
                }
            }

            return entity;
        }

        /// Populates a new instance of the specified type using metadata from a dictionary.
        public static T SetMetadata<T>(T entity, IDictionary<string, object> metadata) where T : new()
        {
            if (metadata == null)
            {
                throw new ArgumentNullException(nameof(metadata), "Metadata cannot be null.");
            }

            // Get cached accessors or generate new ones
            var accessors = _cache.GetOrAdd(typeof(T), GenerateAccessors);

            // Set values from metadata
            foreach (var accessor in accessors.NormalNames.Values)
            {
                if (metadata.TryGetValue(accessor.Name, out var value) && accessor.Setter != null)
                {
                    accessor.Setter(entity, value);
                }
            }

            return entity;
        }

        /// Populates a new instance of the specified type using metadata from a dictionary.
        public static T SetMetadata<T>(T entity, string fieldName, object value, MemberAccessorContainer accessors = null) where T : new()
        {
            // Get cached accessors or generate new ones
            accessors ??= _cache.GetOrAdd(typeof(T), GenerateAccessors);

            if (accessors.NormalNames.TryGetValue(fieldName, out var accessor) == true)
                accessor.Setter(entity, value);

            return entity;
        }

        /// Generates a list of accessors for public properties and fields of a given type.
        private static MemberAccessorContainer GenerateAccessors(Type type)
        {
            var accessors = new Dictionary<string, MemberAccessor>();

            // Process public properties
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var property in properties)
            {
                if (_HasIgnoreAttribute(property))
                    continue;

                var accessor = new MemberAccessor
                {
                    Name = property.Name,
                    Type = property.PropertyType,
                    Getter = CreateGetter(property),
                    Setter = property.CanWrite ? CreateSetter(property) : null
                };
                accessors.Add(property.Name, accessor);
            }

            // Process public fields
            var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
            foreach (var field in fields)
            {
                if (_HasIgnoreAttribute(field))
                    continue;

                var accessor = new MemberAccessor
                {
                    Name = field.Name,
                    Type = field.FieldType,
                    Getter = CreateGetter(field),
                    Setter = CreateSetter(field)
                };
                accessors.Add(field.Name, accessor);
            }

            return new MemberAccessorContainer()
            {
                NormalNames = accessors,
                LowerCaseNames = accessors.ToDictionary(kvp => kvp.Key.ToLower(), kvp => kvp.Value)
            };
        }

        private static bool _HasIgnoreAttribute(MemberInfo member)
        {
            return member.GetCustomAttributes(inherit: true).Any(attr =>
                attr.GetType().Name is "IgnoreAttribute"      // saját [Ignore]
                || attr.GetType().Name is "JsonIgnoreAttribute" // System.Text.Json
                || attr.GetType().Name is "ProtoIgnoreAttribute" // Protobuf
                || attr.GetType().Name is "IgnoreDataMemberAttribute" // DataContractSerializer
                || attr.GetType().Name is "XmlIgnoreAttribute" // XML Serializer
            );
        }

        public static Dictionary<string, MemberAccessor> GetAccessors<T>(bool lowerCaseNames = false)
        {
            var accessors = _cache.GetOrAdd(typeof(T), GenerateAccessors);

            return lowerCaseNames == true
                ? accessors.LowerCaseNames
                : accessors.NormalNames;
        }

        /// Converts a string value to a specified type.
        private static object ConvertToType(string value, Type targetType)
        {
            if (targetType == typeof(string)) return value;
            if (targetType == typeof(int)) return int.Parse(value, CultureInfo.InvariantCulture);
            if (targetType == typeof(bool)) return bool.Parse(value);
            if (targetType == typeof(DateTime)) return DateTime.ParseExact(value, "o", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
            if (targetType == typeof(DateOnly)) return DateOnly.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces);
            if (targetType == typeof(TimeOnly)) return TimeOnly.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces);
            if (targetType.IsEnum) return Enum.Parse(targetType, value);

            return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
        }

        /// Creates a getter delegate for a property.
        private static Func<object, object> CreateGetter(PropertyInfo property)
        {
            var parameter = Expression.Parameter(typeof(object), "instance");
            var castInstance = Expression.Convert(parameter, property.DeclaringType!);
            var propertyAccess = Expression.Property(castInstance, property);
            var convertResult = Expression.Convert(propertyAccess, typeof(object));

            return Expression.Lambda<Func<object, object>>(convertResult, parameter).Compile();
        }

        /// Creates a setter delegate for a property.
        private static Action<object, object> CreateSetter(PropertyInfo property)
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var value = Expression.Parameter(typeof(object), "value");
            var castInstance = Expression.Convert(instance, property.DeclaringType!);
            var castValue = Expression.Convert(value, property.PropertyType);
            var propertyAccess = Expression.Property(castInstance, property);
            var assign = Expression.Assign(propertyAccess, castValue);
            return Expression.Lambda<Action<object, object>>(assign, instance, value).Compile();
        }

        /// Creates a getter delegate for a field.
        private static Func<object, object> CreateGetter(FieldInfo field)
        {
            var parameter = Expression.Parameter(typeof(object), "instance");
            var castInstance = Expression.Convert(parameter, field.DeclaringType!);
            var fieldAccess = Expression.Field(castInstance, field);
            var convertResult = Expression.Convert(fieldAccess, typeof(object));
            return Expression.Lambda<Func<object, object>>(convertResult, parameter).Compile();
        }

        /// Creates a setter delegate for a field.
        private static Action<object, object> CreateSetter(FieldInfo field)
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var value = Expression.Parameter(typeof(object), "value");
            var castInstance = Expression.Convert(instance, field.DeclaringType!);
            var castValue = Expression.Convert(value, field.FieldType);
            var fieldAccess = Expression.Field(castInstance, field);
            var assign = Expression.Assign(fieldAccess, castValue);
            return Expression.Lambda<Action<object, object>>(assign, instance, value).Compile();
        }

        /// Represents an accessor for a property or field.
        public class MemberAccessor
        {
            public string Name { get; set; } = null!;
            public Type Type { get; set; } = null!;
            public Func<object, object> Getter { get; set; } = null!;
            public Action<object, object> Setter { get; set; }
        }
    }
}
