using Azure;
using Azure.Data.Tables;

namespace PolyPersist.Net.BlobStore.AzureBlob.AzureTable
{
    internal class AzureTable_Entity<TEntity> : ITableEntity
        where TEntity : IEntity, new()
    {
        internal TEntity Entity;

        public AzureTable_Entity(TEntity entity) => Entity = entity;
        string ITableEntity.PartitionKey { get => Entity.PartitionKey; set => Entity.PartitionKey = value; }
        string ITableEntity.RowKey { get => Entity.id; set => Entity.id = value; }
        ETag ITableEntity.ETag { get => new(Entity.etag); set => Entity.etag = value.ToString(); }
        DateTimeOffset? ITableEntity.Timestamp { get; set; }
    }
}
