using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Npgsql-backed connection provider that leverages pooled NpgsqlDataSource.
/// </summary>
public sealed class PostgresConnectionProvider : IConnectionProvider, IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresConnectionProvider(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString, nameof(connectionString));
        _dataSource = NpgsqlDataSource.Create(connectionString);
    }

    public async Task<DbConnection> OpenAsync(CancellationToken ct)
    {
        return await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await _dataSource.DisposeAsync().ConfigureAwait(false);
    }
}
