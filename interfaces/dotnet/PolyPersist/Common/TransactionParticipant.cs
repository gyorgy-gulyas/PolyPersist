namespace PolyPersist.Net.Common
{
    /// <summary>
    /// A store whose writes can be run inside a NATIVE database transaction on one connection.
    /// <para>
    /// This is a .NET infrastructure contract and is deliberately NOT part of the language-agnostic
    /// *.contract surface: only <c>ITransaction</c> and the store implementation need to know about
    /// it. Stores that cannot offer a native transaction (Mongo, Cassandra, blob containers) simply
    /// do not implement it and keep being compensated on failure.
    /// </para>
    /// </summary>
    public interface ITransactionParticipant
    {
        /// <summary>
        /// Opens a connection and begins a native transaction on it. The caller owns the returned
        /// scope and must Commit / Rollback / dispose it.
        /// </summary>
        Task<ITransactionScope> BeginScope();
    }

    /// <summary>
    /// One native database transaction, bound to a single connection. Every table obtained through
    /// <see cref="Bind{TRecord}"/> executes on that connection, so a single COMMIT covers all of
    /// them - which is what makes a multi-table transaction possible.
    /// </summary>
    public interface ITransactionScope : IAsyncDisposable
    {
        /// <summary>Returns the same table, rebound to this scope's connection and transaction.</summary>
        ITable<TRecord> Bind<TRecord>(ITable<TRecord> table)
            where TRecord : IRecord, new();

        Task Commit();

        /// <summary>Idempotent: a rollback after a commit (or a second rollback) does nothing.</summary>
        Task Rollback();
    }
}
