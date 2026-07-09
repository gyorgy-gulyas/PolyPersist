namespace PolyPersist.Net.Core
{
    /// <summary>
    /// Concrete, instantiable <see cref="IEvent"/> for callers (event-sourced aggregates, audit) to
    /// create events, and for stores to materialise them on read. The counterpart of
    /// <see cref="Entity"/> for <see cref="IEntity"/>.
    /// </summary>
    public class Event : IEvent
    {
        public string eventId { get; set; } = null!;
        public string streamId { get; set; } = null!;
        public int version { get; set; }
        public string eventType { get; set; } = null!;
        public string data { get; set; } = null!;
        public string metadata { get; set; } = null!;
        public DateTime timestamp { get; set; }
    }
}
