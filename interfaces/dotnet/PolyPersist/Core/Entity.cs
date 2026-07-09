
namespace PolyPersist.Net.Core
{
    public class Entity : IEntity
    {
        public string id { get; set; } = null!;
        public string etag { get; set; } = null!;
        public string PartitionKey { get; set; } = null!;
        public DateTime LastUpdate { get; set; }
    }
}
