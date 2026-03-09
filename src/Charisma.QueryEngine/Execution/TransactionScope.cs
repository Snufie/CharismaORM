using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Transaction wrapper that pins an executor to a single connection/transaction.
/// </summary>
public sealed class TransactionScope : ITransactionScope
{
    private readonly ISqlExecutor _sqlExecutor;
    private readonly DbTransaction _transaction;
    private readonly DbConnection _connection;
    private bool _completed;

    public TransactionScope(ISqlExecutor sqlExecutor, SqlExecutionContext context)
    {
        _sqlExecutor = sqlExecutor ?? throw new ArgumentNullException(nameof(sqlExecutor));
        Context = context ?? throw new ArgumentNullException(nameof(context));
        _connection = context.Connection ?? throw new ArgumentNullException(nameof(context.Connection));
        _transaction = context.Transaction ?? throw new ArgumentNullException(nameof(context.Transaction));
        Executor = new ContextualSqlExecutor(sqlExecutor, context);
    }

    public ISqlExecutor Executor { get; }

    public SqlExecutionContext Context { get; }

    public void FailAndRollback(string? reason = null)
    {
        throw new ManualTransactionRollbackException(reason);
    }

    public async Task CommitAsync(CancellationToken ct = default)
    {
        if (_completed)
        {
            return;
        }

        await _transaction.CommitAsync(ct).ConfigureAwait(false);
        _completed = true;
    }

    public async Task RollbackAsync(CancellationToken ct = default)
    {
        if (_completed)
        {
            return;
        }

        await _transaction.RollbackAsync(ct).ConfigureAwait(false);
        _completed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            await RollbackAsync().ConfigureAwait(false);
        }

        await _transaction.DisposeAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);
    }

    private sealed class ContextualSqlExecutor : ISqlExecutor
    {
        private readonly ISqlExecutor _inner;
        private readonly SqlExecutionContext _context;

        public ContextualSqlExecutor(ISqlExecutor inner, SqlExecutionContext context)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public Task<T?> ExecuteSingleAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
        {
            return _inner.ExecuteSingleAsync<T>(query, _context, ct);
        }

        public Task<IReadOnlyList<T>> ExecuteManyAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
        {
            return _inner.ExecuteManyAsync<T>(query, _context, ct);
        }

        public Task<int> ExecuteNonQueryAsync(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
        {
            return _inner.ExecuteNonQueryAsync(query, _context, ct);
        }

        public Task TransactionAsync(Func<ITransactionScope, Task> work, CancellationToken ct = default)
        {
            return _inner.TransactionAsync(work, ct);
        }

        public Task<T> TransactionAsync<T>(Func<ITransactionScope, Task<T>> work, CancellationToken ct = default)
        {
            return _inner.TransactionAsync(work, ct);
        }

        public Task<ITransactionScope> BeginTransactionAsync(CancellationToken ct = default)
        {
            return _inner.BeginTransactionAsync(ct);
        }
    }
}
