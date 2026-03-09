using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Provider-specific SQL executor abstraction consumed directly by generated delegates.
/// Responsible for planning, executing, and materializing results for the supplied <see cref="QueryModel"/>.
/// </summary>
public interface ISqlExecutor
{
    /// <summary>
    /// Executes a query that should return at most a single row and materializes it to <typeparamref name="T"/>.
    /// </summary>
    /// <param name="query">Query model containing type, model, and args.</param>
    /// <param name="context">Optional execution context for ambient transaction.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Materialized record or null.</returns>
    Task<T?> ExecuteSingleAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default);

    /// <summary>
    /// Executes a query that can return multiple rows and materializes a read-only list of <typeparamref name="T"/>.
    /// </summary>
    /// <param name="query">Query model containing type, model, and args.</param>
    /// <param name="context">Optional execution context for ambient transaction.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Read-only list of materialized records.</returns>
    Task<IReadOnlyList<T>> ExecuteManyAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default);

    /// <summary>
    /// Executes a mutation-only command and returns the affected row count.
    /// </summary>
    /// <param name="query">Query model containing type, model, and args.</param>
    /// <param name="context">Optional execution context for ambient transaction.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Affected row count.</returns>
    Task<int> ExecuteNonQueryAsync(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default);

    /// <summary>
    /// Runs a transactional unit of work using the ambient executor.
    /// </summary>
    /// <param name="work">Delegate executed within a transaction scope.</param>
    /// <param name="ct">Cancellation token.</param>
    Task TransactionAsync(Func<ITransactionScope, Task> work, CancellationToken ct = default);

    /// <summary>
    /// Runs a transactional unit of work that produces a result.
    /// </summary>
    /// <param name="work">Delegate executed within a transaction scope.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Result produced by the delegate.</returns>
    Task<T> TransactionAsync<T>(Func<ITransactionScope, Task<T>> work, CancellationToken ct = default);

    /// <summary>
    /// Begins a transaction and returns its scope.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Ambient transaction scope.</returns>
    Task<ITransactionScope> BeginTransactionAsync(CancellationToken ct = default);
}