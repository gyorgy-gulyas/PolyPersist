namespace PolyPersist.Net.Core
{
    public class Blob : Entity, IBlob
    {
        public string fileName { get; set; }
        public string contentType { get; set; }
    }
}
