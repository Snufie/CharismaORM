using System;

namespace Charisma.QueryEngine.Exceptions;

/// <summary>
/// Base exception for transactional failures.
/// </summary>
public class CharismaTransactionException : Exception
{
    /// <summary>
    /// Creates a transaction exception with an optional inner cause.
    /// </summary>
    /// <param name="message">Human-readable reason.</param>
    /// <param name="inner">Optional underlying exception.</param>
    public CharismaTransactionException(string message, Exception? inner = null)
        : base(message, inner)
    {
    }
}

/// <summary>
/// Thrown when a transaction is manually failed by user code.
/// </summary>
public sealed class ManualTransactionRollbackException : CharismaTransactionException
{
    /// <summary>
    /// Creates a rollback exception with an optional reason.
    /// </summary>
    /// <param name="reason">Optional message describing why the transaction was rolled back.</param>
    public ManualTransactionRollbackException(string? reason = null)
        : base(reason ?? "Transaction was manually rolled back.")
    {
    }
}
