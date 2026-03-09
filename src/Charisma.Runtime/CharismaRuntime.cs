using System;
using System.Reflection;
using System.Threading.Tasks;
using Charisma.QueryEngine.Execution;

namespace Charisma.Runtime;

/// <summary>
/// Composes provider-specific SQL executor consumed by generated delegates.
/// </summary>
public sealed class CharismaRuntime : IDisposable
{
    private readonly ISqlExecutor _sqlExecutor;
    private readonly IConnectionProvider _connectionProvider;
    private readonly bool _ownsConnectionProvider;

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

        _connectionProvider = options.ConnectionProvider ?? new PostgresConnectionProvider(options.ConnectionString);
        _ownsConnectionProvider = options.ConnectionProvider is null;

        _sqlExecutor = options.Provider switch
        {
            ProviderOptions.PostgreSQL => BuildPostgresExecutor(options, _connectionProvider),
            _ => throw new NotSupportedException($"Provider '{options.Provider}' is not supported.")
        };
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
