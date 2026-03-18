using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Charisma.QueryEngine.Planning;
using Npgsql;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Npgsql-backed executor for Phase 2.2. Translates <see cref="QueryModel"/> instances
/// into SQL using generated metadata and materializes POCOs from the caller's assembly.
/// </summary>
public sealed partial class PostgresSqlExecutor : ISqlExecutor, IAsyncDisposable
{
    private readonly IConnectionProvider _connectionProvider;
    private readonly bool _ownsConnectionProvider;
    private readonly IReadOnlyDictionary<string, ModelMetadata> _metadata;
    private readonly Func<string, Type> _modelTypeResolver;
    private readonly PostgresSqlPlanner _planner;
    private readonly Assembly _generatedAssembly;
    private readonly string _rootNamespace;
    private readonly bool _preserveIdentifierCasing;
    private readonly int _maxNestingDepth;
    private readonly IReadOnlyDictionary<string, object?>? _globalOmit;

    /// <summary>
    /// Creates a Postgres-backed executor using the provided options and generated metadata.
    /// </summary>
    /// <param name="options">Runtime configuration including connection string, root namespace, and registry overrides.</param>
    /// <exception cref="ArgumentException">Thrown when required options are missing.</exception>
    public PostgresSqlExecutor(PostgresExecutorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.RootNamespace))
        {
            throw new ArgumentException("Root namespace is required to resolve generated types.", nameof(options));
        }

        if (options.ConnectionProvider is null && string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("Connection string is required when no connection provider is supplied.", nameof(options));
        }

        var assembly = options.GeneratedAssembly ?? Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly();
        _generatedAssembly = assembly;
        _rootNamespace = options.RootNamespace;
        _metadata = options.MetadataRegistry ?? LoadMetadata(options.RootNamespace, assembly);
        _maxNestingDepth = options.MaxNestingDepth;
        _globalOmit = options.GlobalOmit;
        _modelTypeResolver = options.ModelTypeResolver ?? (name =>
        {
            var type = assembly.GetType($"{options.RootNamespace}.Models.{name}");
            if (type is null)
            {
                throw new InvalidOperationException($"Model type '{name}' not found in assembly '{assembly.FullName}'.");
            }
            return type;
        });

        if (options.ConnectionProvider is not null)
        {
            _connectionProvider = options.ConnectionProvider;
            _ownsConnectionProvider = false;
        }
        else
        {
            var connectionString = options.ConnectionString ?? throw new ArgumentException("Connection string is required when no connection provider is supplied.", nameof(options));
            _connectionProvider = new PostgresConnectionProvider(connectionString);
            _ownsConnectionProvider = true;
        }

        _preserveIdentifierCasing = options.PreserveIdentifierCasing;
        _planner = new PostgresSqlPlanner(_metadata, _preserveIdentifierCasing, _maxNestingDepth, _globalOmit);
    }

    /// <summary>
    /// Disposes the underlying Npgsql data source.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_ownsConnectionProvider)
        {
            switch (_connectionProvider)
            {
                case IAsyncDisposable asyncDisposable:
                    await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                    break;
                case IDisposable disposable:
                    disposable.Dispose();
                    break;
            }
        }
    }

    /// <inheritdoc />
    public async Task<T?> ExecuteSingleAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        EnsureSingleQueryType(query.Type);
        QueryValidator.Validate(query);

        var result = await ExecuteQueryInternalAsync(query, context, ct).ConfigureAwait(false);
        if (result is null)
        {
            return default;
        }

        if (result is T typed)
        {
            return typed;
        }

        throw new InvalidOperationException(BuildTypeMismatchMessage(typeof(T), result.GetType(), query));
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<T>> ExecuteManyAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        EnsureManyQueryType(query.Type);
        QueryValidator.Validate(query);

        if (query is CreateManyQueryModel { ReturnRecords: false } || query is UpdateManyQueryModel { ReturnRecords: false })
        {
            throw new NotSupportedException("Use ExecuteNonQueryAsync for CreateMany/UpdateMany when ReturnRecords is false.");
        }

        if (query.Type == QueryType.FindMany)
        {
            return await ExecutePlannedQueryManyTypedAsync<T>(query, context, ct).ConfigureAwait(false);
        }

        var result = await ExecuteQueryInternalAsync(query, context, ct).ConfigureAwait(false);
        return CastToReadOnlyList<T>(result, query);
    }

    /// <inheritdoc />
    public Task<int> ExecuteNonQueryAsync(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(query);
        EnsureNonQueryType(query.Type);
        QueryValidator.Validate(query);
        return ExecuteCommandInternalAsync(query, context, ct);
    }

    /// <inheritdoc />
    public async Task TransactionAsync(Func<ITransactionScope, Task> work, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(work);

        await using var scope = await BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            await work(scope).ConfigureAwait(false);
            await scope.CommitAsync(ct).ConfigureAwait(false);
        }
        catch (ManualTransactionRollbackException)
        {
            await scope.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            await scope.RollbackAsync(ct).ConfigureAwait(false);
            throw new CharismaTransactionException("Transaction failed.", ex);
        }
    }

    /// <inheritdoc />
    public async Task<T> TransactionAsync<T>(Func<ITransactionScope, Task<T>> work, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(work);

        await using var scope = await BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            var result = await work(scope).ConfigureAwait(false);
            await scope.CommitAsync(ct).ConfigureAwait(false);
            return result;
        }
        catch (ManualTransactionRollbackException)
        {
            await scope.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex)
        {
            await scope.RollbackAsync(ct).ConfigureAwait(false);
            throw new CharismaTransactionException("Transaction failed.", ex);
        }
    }

    public async Task<ITransactionScope> BeginTransactionAsync(CancellationToken ct = default)
    {
        var dbConn = await _connectionProvider.OpenAsync(ct).ConfigureAwait(false);
        if (dbConn is not NpgsqlConnection conn)
        {
            await dbConn.DisposeAsync().ConfigureAwait(false);
            throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
        }

        var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);
        var context = new SqlExecutionContext(conn, tx);
        return new TransactionScope(this, context);
    }

    private async Task<T> WithTransactionAsync<T>(Func<NpgsqlConnection, NpgsqlTransaction, Task<T>> action, CancellationToken ct, NpgsqlConnection? ambientConn = null, NpgsqlTransaction? ambientTx = null)
    {
        if (ambientConn is not null && ambientTx is not null)
        {
            return await action(ambientConn, ambientTx).ConfigureAwait(false);
        }

        await using var dbConn = await _connectionProvider.OpenAsync(ct).ConfigureAwait(false);
        if (dbConn is not NpgsqlConnection conn)
        {
            throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
        }

        await using var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);
        try
        {
            var result = await action(conn, tx).ConfigureAwait(false);
            await tx.CommitAsync(ct).ConfigureAwait(false);
            return result;
        }
        catch
        {
            await tx.RollbackAsync(ct).ConfigureAwait(false);
            throw;
        }
    }

    private static (NpgsqlConnection? Conn, NpgsqlTransaction? Tx) ExtractContext(SqlExecutionContext? context)
    {
        if (context is null)
        {
            return (null, null);
        }

        if (context.Connection is not NpgsqlConnection conn)
        {
            throw new InvalidOperationException("Ambient execution context does not carry an NpgsqlConnection.");
        }

        return (conn, context.Transaction as NpgsqlTransaction);
    }

    private ModelMetadata GetModelMetadata(string modelName)
    {
        if (!_metadata.TryGetValue(modelName, out var meta))
        {
            throw new InvalidOperationException($"Model metadata for '{modelName}' was not found.");
        }
        return meta;
    }

    /// <summary>
    /// Resolves the generated aggregate result type for the provided model name.
    /// </summary>
    private Type ResolveAggregateResultType(string modelName)
    {
        var type = _generatedAssembly.GetType($"{_rootNamespace}.Args.{modelName}AggregateResult");
        if (type is null)
        {
            throw new InvalidOperationException($"Aggregate result type for model '{modelName}' could not be resolved. Expected '{_rootNamespace}.Args.{modelName}AggregateResult'.");
        }

        return type;
    }

    /// <summary>
    /// Resolves the generated group-by result type for the provided model name.
    /// </summary>
    private Type ResolveGroupByResultType(string modelName)
    {
        var type = _generatedAssembly.GetType($"{_rootNamespace}.Args.{modelName}GroupByOutput");
        if (type is null)
        {
            throw new InvalidOperationException($"GroupBy result type for model '{modelName}' could not be resolved. Expected '{_rootNamespace}.Args.{modelName}GroupByOutput'.");
        }

        return type;
    }

    private static void EnsureSingleQueryType(QueryType type)
    {
        switch (type)
        {
            case QueryType.FindUnique:
            case QueryType.FindFirst:
            case QueryType.Create:
            case QueryType.Update:
            case QueryType.Delete:
            case QueryType.Upsert:
            case QueryType.Count:
            case QueryType.Aggregate:
                return;
            case QueryType.GroupBy:
                return;
            default:
                throw new NotSupportedException($"QueryType '{type}' is not supported for single-result execution.");
        }
    }

    private static void EnsureManyQueryType(QueryType type)
    {
        switch (type)
        {
            case QueryType.FindMany:
            case QueryType.CreateMany:
            case QueryType.UpdateMany:
            case QueryType.GroupBy:
                return;
            default:
                throw new NotSupportedException($"QueryType '{type}' is not supported for multi-result execution.");
        }
    }

    private static void EnsureNonQueryType(QueryType type)
    {
        switch (type)
        {
            case QueryType.CreateMany:
            case QueryType.UpdateMany:
            case QueryType.DeleteMany:
                return;
            default:
                throw new NotSupportedException($"QueryType '{type}' is not supported for non-query execution.");
        }
    }
}

/// <summary>
/// Options for configuring <see cref="PostgresSqlExecutor"/>.
/// </summary>
public sealed class PostgresExecutorOptions
{
    public string? ConnectionString { get; init; }
    public required string RootNamespace { get; init; }
    public IConnectionProvider? ConnectionProvider { get; init; }
    public Assembly? GeneratedAssembly { get; init; }
    public IReadOnlyDictionary<string, ModelMetadata>? MetadataRegistry { get; init; }
    public Func<string, Type>? ModelTypeResolver { get; init; }
    public bool PreserveIdentifierCasing { get; init; }
    public int MaxNestingDepth { get; init; } = 12;
    public IReadOnlyDictionary<string, object?>? GlobalOmit { get; init; }
}
