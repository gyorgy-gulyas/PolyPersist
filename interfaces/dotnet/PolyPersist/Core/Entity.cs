
namespace PolyPersist.Net.Core
{
    public class Entity : IEntity
    {
        public string id { get; set; }
        public string etag { get; set; }
        public string PartitionKey { get; set; }
        public DateTime LastUpdate { get; set; }
    }
}
