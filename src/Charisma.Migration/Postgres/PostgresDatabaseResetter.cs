using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Drops and recreates the public schema to provide a clean slate for pushes.
/// </summary>
public sealed class PostgresDatabaseResetter
{
    private readonly string _connectionString;

    public PostgresDatabaseResetter(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public async Task ResetAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"drop schema if exists public cascade; create schema public;";

        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
