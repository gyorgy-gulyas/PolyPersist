namespace PolyPersist.Net.Core
{
    /// <summary>
    /// Concrete, instantiable <see cref="IEvent"/> for callers (event-sourced aggregates, audit) to
    /// create events, and for stores to materialise them on read. The counterpart of
    /// <see cref="Entity"/> for <see cref="IEntity"/>.
    /// </summary>
    public class Event : IEvent
    {
        public string eventId { get; set; }
        public string streamId { get; set; }
        public int version { get; set; }
        public string eventType { get; set; }
        public string data { get; set; }
        public string metadata { get; set; }
        public DateTime timestamp { get; set; }
    }
}
