using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Execution;
using Charisma.QueryEngine.Model;
using Xunit;

namespace Charisma.QueryEngine.Tests;

public class TransactionScopeTests
{
    [Fact]
    public async Task ContextualExecutor_ForwardsAmbientContext()
    {
        var executor = new RecordingSqlExecutor();
        var conn = new FakeDbConnection();
        var tx = new TrackingDbTransaction(conn);
        var context = new SqlExecutionContext(conn, tx);

        await using var scope = new TransactionScope(executor, context);
        var query = new FindUniqueQueryModel("Robot", new { });

        await scope.Executor.ExecuteNonQueryAsync(query);

        Assert.Same(context, executor.LastContext);
    }

    [Fact]
    public async Task CommitAsync_CommitsUnderlyingTransaction()
    {
        var executor = new RecordingSqlExecutor();
        var conn = new FakeDbConnection();
        var tx = new TrackingDbTransaction(conn);
        var context = new SqlExecutionContext(conn, tx);

        await using var scope = new TransactionScope(executor, context);
        await scope.CommitAsync();

        Assert.True(tx.Committed);
        Assert.False(tx.RolledBack);
    }

    [Fact]
    public async Task DisposeAsync_RollsBackWhenNotCompleted()
    {
        var executor = new RecordingSqlExecutor();
        var conn = new FakeDbConnection();
        var tx = new TrackingDbTransaction(conn);
        var context = new SqlExecutionContext(conn, tx);

        var scope = new TransactionScope(executor, context);
        await scope.DisposeAsync();

        Assert.True(tx.RolledBack);
    }

    [Fact]
    public async Task FailAndRollback_ThrowsAndTriggersRollbackOnDispose()
    {
        var executor = new RecordingSqlExecutor();
        var conn = new FakeDbConnection();
        var tx = new TrackingDbTransaction(conn);
        var context = new SqlExecutionContext(conn, tx);

        await using var scope = new TransactionScope(executor, context);

        await Assert.ThrowsAsync<ManualTransactionRollbackException>(() => Task.Run(() => scope.FailAndRollback("fail")));

        await scope.DisposeAsync();

        Assert.True(tx.RolledBack);
    }

    private sealed class RecordingSqlExecutor : ISqlExecutor
    {
        public SqlExecutionContext? LastContext { get; private set; }
        public int NextNonQueryResult { get; set; } = 1;

        public Task<T?> ExecuteSingleAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
        {
            LastContext = context;
            return Task.FromResult(default(T));
        }

        public Task<IReadOnlyList<T>> ExecuteManyAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
        {
            LastContext = context;
            return Task.FromResult<IReadOnlyList<T>>(Array.Empty<T>());
        }

        public Task<int> ExecuteNonQueryAsync(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
        {
            LastContext = context;
            return Task.FromResult(NextNonQueryResult);
        }

        public Task TransactionAsync(Func<ITransactionScope, Task> work, CancellationToken ct = default)
        {
            throw new NotSupportedException();
        }

        public Task<T> TransactionAsync<T>(Func<ITransactionScope, Task<T>> work, CancellationToken ct = default)
        {
            throw new NotSupportedException();
        }

        public Task<ITransactionScope> BeginTransactionAsync(CancellationToken ct = default)
        {
            throw new NotSupportedException();
        }
    }

    private sealed class FakeDbConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Open;

        private string _connectionString = string.Empty;
        [AllowNull]
        public override string ConnectionString
        {
            get => _connectionString;
            set => _connectionString = value ?? string.Empty;
        }
        public override string Database => "Fake";
        public override string DataSource => "Fake";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) { }

        public override void Close() => _state = ConnectionState.Closed;

        public override void Open() => _state = ConnectionState.Open;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class TrackingDbTransaction : DbTransaction
    {
        private readonly DbConnection _connection;

        public TrackingDbTransaction(DbConnection connection)
        {
            _connection = connection;
        }

        public bool Committed { get; private set; }
        public bool RolledBack { get; private set; }

        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection DbConnection => _connection;

        public override void Commit() => Committed = true;

        public override void Rollback() => RolledBack = true;

        public override Task CommitAsync(CancellationToken cancellationToken = default)
        {
            Commit();
            return Task.CompletedTask;
        }

        public override Task RollbackAsync(CancellationToken cancellationToken = default)
        {
            Rollback();
            return Task.CompletedTask;
        }
    }
}
