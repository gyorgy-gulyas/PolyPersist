namespace PolyPersist.Net.Common
{
    /// <summary>
    /// Base type for every error PolyPersist itself raises. Catching this lets a caller handle any
    /// storage-contract failure as one family, while the concrete subtypes below let it react to a
    /// specific case (not-found vs concurrency vs duplicate vs bad request) without matching on a
    /// human-readable message. Provider/driver faults (network, SQL syntax, ...) are NOT wrapped in
    /// this hierarchy; they surface as their native exception types.
    /// </summary>
    public class PolyPersistException : Exception
    {
        public PolyPersistException(string message) : base(message) { }
        public PolyPersistException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// The target of a Find / Delete / Update (or a table / collection / container / index lookup or
    /// drop) does not exist. The identity (partitionKey, id) or the named store object was not found.
    /// </summary>
    public class NotFoundException : PolyPersistException
    {
        public NotFoundException(string message) : base(message) { }
        public NotFoundException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// An Insert / Upload used an id that already exists, or a CreateTable / CreateCollection /
    /// CreateContainer used a name that is already taken. The key or name collides with a stored one.
    /// </summary>
    public class DuplicateKeyException : PolyPersistException
    {
        public DuplicateKeyException(string message) : base(message) { }
        public DuplicateKeyException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Optimistic-concurrency failure: the stored etag no longer matches the one the caller last read,
    /// so the row moved on under them. Typically the caller should reload and retry.
    /// </summary>
    public class ConcurrencyConflictException : PolyPersistException
    {
        public ConcurrencyConflictException(string message) : base(message) { }
        public ConcurrencyConflictException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// The request was rejected before it reached storage because the entity or its arguments were not
    /// in a valid state for the operation: a missing PartitionKey, an etag present on Insert or absent
    /// on Update, or unreadable content on Upload.
    /// </summary>
    public class InvalidRequestException : PolyPersistException
    {
        public InvalidRequestException(string message) : base(message) { }
        public InvalidRequestException(string message, Exception innerException) : base(message, innerException) { }
    }
}
