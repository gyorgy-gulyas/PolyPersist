namespace PolyPersist.Net.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class ClusteringColumnAttribute : Attribute
    {
        public readonly int _clusteringOrder;
        public ClusteringColumnAttribute(int clusteringOrder)
        {
            _clusteringOrder = clusteringOrder;
        }
    }
}
