using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Migration;
using Charisma.Migration.Postgres;
using Charisma.QueryEngine.Execution;
using Charisma.Schema;

namespace Charisma.Runtime;

/// <summary>
/// Composes provider-specific SQL executor consumed by generated delegates.
/// </summary>
public sealed class CharismaRuntime : IDisposable
{
    private readonly ISqlExecutor _sqlExecutor;
    private readonly IConnectionProvider _connectionProvider;
    private readonly bool _ownsConnectionProvider;
    private readonly string _connectionString;
    private readonly ProviderOptions _provider;

    public IConnectionProvider ConnectionProvider => _connectionProvider;
    public ISqlExecutor SqlExecutor => _sqlExecutor;

    /// <summary>
    /// Constructs a runtime with the configured provider-specific SQL executor.
    /// </summary>
    /// <param name="options">Configuration including provider, connection string, and generated assembly overrides.</param>
    /// <exception cref="ArgumentException">Thrown when required options are missing.</exception>
    public CharismaRuntime(CharismaRuntimeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (string.IsNullOrWhiteSpace(options.ConnectionString) && options.ConnectionProvider is null)
        {
            throw new ArgumentException("ConnectionString is required when no ConnectionProvider is supplied.", nameof(options));
        }
        if (string.IsNullOrWhiteSpace(options.RootNamespace))
        {
            throw new ArgumentException("RootNamespace is required to resolve generated code.", nameof(options));
        }

        _connectionString = options.ConnectionString;
        _provider = options.Provider;
        _connectionProvider = options.ConnectionProvider ?? new PostgresConnectionProvider(options.ConnectionString);
        _ownsConnectionProvider = options.ConnectionProvider is null;

        _sqlExecutor = options.Provider switch
        {
            ProviderOptions.PostgreSQL => BuildPostgresExecutor(options, _connectionProvider),
            _ => throw new NotSupportedException($"Provider '{options.Provider}' is not supported.")
        };
    }

    /// <summary>
    /// Plans and applies schema migrations for the configured provider. Intended for app startup flows.
    /// </summary>
    /// <param name="schema">Desired schema state to migrate to.</param>
    /// <param name="options">Migration safety options. Defaults to conservative settings.</param>
    /// <param name="cancellationToken">Cancellation token for the migration operation.</param>
    /// <returns>The computed migration plan that was applied (or empty when already in sync).</returns>
    public async Task<MigrationPlan> MigrateAsync(
        CharismaSchema schema,
        PostgresMigrationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);

        if (_provider != ProviderOptions.PostgreSQL)
        {
            throw new NotSupportedException($"Startup migration is only supported for provider '{ProviderOptions.PostgreSQL}'.");
        }

        var migrationOptions = options ?? new PostgresMigrationOptions();
        var planner = new PostgresMigrationPlanner(new PostgresIntrospectionOptions(_connectionString), migrationOptions);
        var plan = await planner.PlanAsync(schema, cancellationToken).ConfigureAwait(false);

        if (plan.Steps.Count == 0)
        {
            return plan;
        }

        var runner = new PostgresMigrationRunner(_connectionString);
        await runner.ExecuteAsync(plan, migrationOptions, cancellationToken).ConfigureAwait(false);
        return plan;
    }

    /// <summary>
    /// Creates the PostgreSQL executor with supplied metadata and resolver overrides.
    /// </summary>
    private static PostgresSqlExecutor BuildPostgresExecutor(CharismaRuntimeOptions options, IConnectionProvider connectionProvider)
    {
        return new PostgresSqlExecutor(new PostgresExecutorOptions
        {
            ConnectionString = options.ConnectionString,
            ConnectionProvider = connectionProvider,
            RootNamespace = options.RootNamespace,
            GeneratedAssembly = options.GeneratedAssembly ?? Assembly.GetEntryAssembly() ?? Assembly.GetExecutingAssembly(),
            MetadataRegistry = options.MetadataRegistry,
            ModelTypeResolver = options.ModelTypeResolver,
            PreserveIdentifierCasing = options.PreserveIdentifierCasing,
            MaxNestingDepth = options.MaxNestingDepth,
            GlobalOmit = options.GlobalOmit
        });
    }

    /// <summary>
    /// Disposes the runtime and underlying executor resources.
    /// </summary>
    public void Dispose()
    {
        switch (_sqlExecutor)
        {
            case IAsyncDisposable asyncDisposable:
                asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }

        if (_ownsConnectionProvider)
        {
            switch (_connectionProvider)
            {
                case IAsyncDisposable asyncDisposable:
                    asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    break;
                case IDisposable disposable:
                    disposable.Dispose();
                    break;
            }
        }
    }
}
