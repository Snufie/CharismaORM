using System.Threading;
using System.Threading.Tasks;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Represents an ambient transaction tied to a single connection.
/// Provides an executor bound to that transaction and a manual rollback hook.
/// </summary>
public interface ITransactionScope : IAsyncDisposable
{
    /// <summary>
    /// Executor pinned to the ambient transaction.
    /// </summary>
    ISqlExecutor Executor { get; }

    /// <summary>
    /// Execution context containing the active connection and transaction.
    /// </summary>
    SqlExecutionContext Context { get; }

    /// <summary>
    /// Explicitly fail and roll back the transaction.
    /// Throws a ManualTransactionRollbackException to stop execution.
    /// </summary>
    /// <param name="reason">Optional reason to include in the exception.</param>
    void FailAndRollback(string? reason = null);

    /// <summary>
    /// Commits the transaction.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task CommitAsync(CancellationToken ct = default);

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task RollbackAsync(CancellationToken ct = default);
}
