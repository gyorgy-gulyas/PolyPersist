using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace PolyPersist.Net.RelationalStore.Dapper
{
    /// <summary>The contract-level meaning of a native SQL error, once classified.</summary>
    internal enum SqlErrorKind
    {
        /// <summary>Not a recognised constraint failure - a driver/network/syntax fault. Left native.</summary>
        Unknown,
        /// <summary>A primary-key or unique-index collision: the row's key is already taken.</summary>
        DuplicateKey,
        /// <summary>Some other constraint the row breaks: check, not-null, foreign key, ...</summary>
        ConstraintViolation,
    }

    /// <summary>
    /// Classifies a provider's native SQL exception so the store can raise the contract's own
    /// exception types instead of leaking <c>SqliteException</c> / <c>PostgresException</c> to
    /// callers (PP-57). Only constraint failures are classified; everything else stays native,
    /// which is what <see cref="PolyPersist.Net.Common.PolyPersistException"/> promises.
    /// <para>
    /// Two channels are read. SQLSTATE (<see cref="DbException.SqlState"/>) is the standard ADO.NET
    /// surface and covers PostgreSQL and any other SQLSTATE-speaking driver without referencing it,
    /// so a new provider usually needs no change here. SQLite does not report SQLSTATE at all, so
    /// its numeric result codes are read from the concrete exception type.
    /// </para>
    /// </summary>
    internal static class SqlErrorTranslator
    {
        // SQLSTATE: class 23 is "integrity constraint violation"; 23505 is specifically unique_violation.
        private const string SqlStateUniqueViolation = "23505";
        private const string SqlStateIntegrityClass = "23";

        // SQLite result codes (SQLITE_CONSTRAINT and its extended forms).
        private const int SqliteConstraint = 19;
        private const int SqliteConstraintPrimaryKey = 1555;
        private const int SqliteConstraintUnique = 2067;

        /// <summary>
        /// Walks the exception chain - linq2db and Dapper may hand the driver's exception back
        /// wrapped - and returns the first recognised constraint failure.
        /// </summary>
        internal static SqlErrorKind Classify(Exception? exception)
        {
            for (Exception? ex = exception; ex is not null; ex = ex.InnerException)
            {
                var kind = _ClassifyOne(ex);
                if (kind != SqlErrorKind.Unknown)
                    return kind;
            }

            return SqlErrorKind.Unknown;
        }

        private static SqlErrorKind _ClassifyOne(Exception ex)
        {
            if (ex is SqliteException sqlite)
            {
                // The extended code says WHICH constraint broke; the base code only says that one did.
                if (sqlite.SqliteExtendedErrorCode is SqliteConstraintPrimaryKey or SqliteConstraintUnique)
                    return SqlErrorKind.DuplicateKey;

                if (sqlite.SqliteErrorCode == SqliteConstraint)
                    return SqlErrorKind.ConstraintViolation;

                return SqlErrorKind.Unknown;
            }

            string? sqlState = (ex as DbException)?.SqlState;
            if (string.IsNullOrEmpty(sqlState) == true)
                return SqlErrorKind.Unknown;

            if (sqlState == SqlStateUniqueViolation)
                return SqlErrorKind.DuplicateKey;

            if (sqlState.StartsWith(SqlStateIntegrityClass, StringComparison.Ordinal) == true)
                return SqlErrorKind.ConstraintViolation;

            return SqlErrorKind.Unknown;
        }
    }
}
