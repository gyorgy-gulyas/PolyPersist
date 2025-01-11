using Azure.Storage.Blobs;
using PolyPersist.Net.Common;
using System.Collections.Concurrent;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;

namespace PolyPersist.Net.BlobStore.AzureBlob
{
    internal class AzureStorage_BlobContainer<TBlob> : IBlobContainer<TBlob>
        where TBlob : IBlob, new()
    {
        private BlobContainerClient _containerClient;

        public AzureStorage_BlobContainer(BlobContainerClient containerClient)
        {
            _containerClient = containerClient;
        }

        /// <inheritdoc/>
        string IBlobContainer<TBlob>.Name => _containerClient.Name;

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Upload(TBlob blob, Stream content)
        {
            await CollectionCommon.CheckBeforeInsert(blob).ConfigureAwait(false);

            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));

            // new etag
            blob.etag = Guid.NewGuid().ToString();

            // set metadata
            await blobClient.SetMetadataAsync(MetadataHelper.GetMetadata(blob)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        async Task<Stream> IBlobContainer<TBlob>.Download(TBlob blob)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be download because it does not exist.");

            // Download blob content
            var response = await blobClient.DownloadAsync();
            return response.Value.Content;
        }

        /// <inheritdoc/>
        async Task<TBlob> IBlobContainer<TBlob>.Find(string partitionKey, string id)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(partitionKey,id));

            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be updated because it does not exist.");

            var properties = await blobClient.GetPropertiesAsync().ConfigureAwait(false);

            return MetadataHelper.SetMetadata<TBlob>(properties.Value.Metadata);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.Delete(string partitionKey, string id)
        {
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(partitionKey, id));

            var response = await blobClient.DeleteIfExistsAsync().ConfigureAwait(false);
            if(response.Value == false )
                throw new Exception($"Blob '{typeof(TBlob).Name}' {id} can not be delete because it does not exist.");
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateContent(TBlob blob, Stream content)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            // Upload new content (overwrite)
            await blobClient.UploadAsync(content, overwrite: true);
        }

        /// <inheritdoc/>
        async Task IBlobContainer<TBlob>.UpdateMetadata(TBlob blob)
        {
            // create blob client
            BlobClient blobClient = _containerClient.GetBlobClient(_makePath(blob));
            if (await blobClient.ExistsAsync().ConfigureAwait(false) == false)
                throw new Exception($"Blob '{typeof(TBlob).Name}' {blob.id} can not be updated because it does not exist.");

            // set metadata
            await blobClient.SetMetadataAsync(MetadataHelper.GetMetadata(blob)).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        object IBlobContainer<TBlob>.GetUnderlyingImplementation()
        {
            return _containerClient;
        }

        private string _makePath(IEntity entity)
            => _makePath(entity.PartitionKey, entity.id);

        private string _makePath(string partitionKey, string id)
            => $"{partitionKey}/{id}";
    }


    public static class MetadataHelper
    {
        // Static cache to store property and field access delegates
        private static readonly ConcurrentDictionary<Type, List<MemberAccessor>> _cache = new();

        /// Retrieves metadata from a given object, including its public properties and fields, as string values.
        public static IDictionary<string, string> GetMetadata<TBlob>(TBlob blob)
        {
            if (blob == null)
            {
                throw new ArgumentNullException(nameof(blob), "The blob object cannot be null.");
            }

            var metadata = new Dictionary<string, string>();

            // Get cached accessors or generate new ones
            var accessors = _cache.GetOrAdd(typeof(TBlob), GenerateAccessors);

            // Execute accessors to extract metadata
            foreach (var accessor in accessors)
            {
                var value = accessor.Getter(blob);
                metadata[accessor.Name] = value?.ToString() ?? string.Empty;
            }

            return metadata;
        }

        /// Populates a new instance of the specified type using metadata from a dictionary.
        public static TBlob SetMetadata<TBlob>(IDictionary<string, string> metadata) where TBlob : new()
        {
            if (metadata == null)
            {
                throw new ArgumentNullException(nameof(metadata), "Metadata cannot be null.");
            }

            // Create a new instance of the target type
            var blob = new TBlob();

            // Get cached accessors or generate new ones
            var accessors = _cache.GetOrAdd(typeof(TBlob), GenerateAccessors);

            // Set values from metadata
            foreach (var accessor in accessors)
            {
                if (metadata.TryGetValue(accessor.Name, out var value) && accessor.Setter != null)
                {
                    var convertedValue = ConvertToType(value, accessor.Type);
                    accessor.Setter(blob, convertedValue);
                }
            }

            return blob;
        }

        /// Generates a list of accessors for public properties and fields of a given type.
        private static List<MemberAccessor> GenerateAccessors(Type type)
        {
            var accessors = new List<MemberAccessor>();

            // Process public properties
            var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var property in properties)
            {
                var accessor = new MemberAccessor
                {
                    Name = property.Name,
                    Type = property.PropertyType,
                    Getter = CreateGetter(property),
                    Setter = property.CanWrite ? CreateSetter(property) : null
                };
                accessors.Add(accessor);
            }

            // Process public fields
            var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
            foreach (var field in fields)
            {
                var accessor = new MemberAccessor
                {
                    Name = field.Name,
                    Type = field.FieldType,
                    Getter = CreateGetter(field),
                    Setter = CreateSetter(field)
                };
                accessors.Add(accessor);
            }

            return accessors;
        }

        /// Converts a string value to a specified type.
        private static object ConvertToType(string value, Type targetType)
        {
            if (targetType == typeof(string)) return value;
            if (targetType == typeof(int)) return int.Parse(value, CultureInfo.InvariantCulture);
            if (targetType == typeof(bool)) return bool.Parse(value);
            if (targetType == typeof(DateTime)) return DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
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
        private class MemberAccessor
        {
            public string Name { get; set; } = null!;
            public Type Type { get; set; } = null!;
            public Func<object, object> Getter { get; set; } = null!;
            public Action<object, object> Setter { get; set; }
        }
    }
}
