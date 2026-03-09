using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Utility to ensure destructive operations are only allowed on empty tables unless forced.
/// </summary>
internal sealed class DropSafetyChecker
{
    private readonly string _connectionString;

    public DropSafetyChecker(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public async Task<bool> IsTableEmptyAsync(string tableName, CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new NpgsqlCommand($"select count(*) from \"{tableName}\"", conn);
        var countObj = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        var count = Convert.ToInt64(countObj);
        return count == 0;
    }

    public async Task<bool> ColumnHasNullsAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new NpgsqlCommand($"select exists(select 1 from \"{tableName}\" where \"{columnName}\" is null limit 1)", conn);
        var existsObj = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return existsObj is bool b && b;
    }

    public async Task<bool> TableHasReferencingRowsAsync(string tableName, CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"select
                kcu.table_name as child_table,
                array_agg(kcu.column_name order by kcu.ordinal_position) as child_columns
            from information_schema.referential_constraints rc
            join information_schema.key_column_usage kcu on rc.constraint_name = kcu.constraint_name and rc.constraint_schema = kcu.constraint_schema
            join information_schema.constraint_column_usage ccu on rc.constraint_name = ccu.constraint_name and rc.constraint_schema = ccu.constraint_schema
            where ccu.table_schema = 'public' and kcu.table_schema = 'public' and ccu.table_name = @table
            group by kcu.table_name";

        var childRefs = new List<(string Table, string[] Columns)>();

        await using (var cmd = new NpgsqlCommand(sql, conn))
        {
            cmd.Parameters.AddWithValue("table", tableName);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var childTable = reader.GetString(reader.GetOrdinal("child_table"));
                var childColumns = reader.GetFieldValue<string[]>(reader.GetOrdinal("child_columns"));
                childRefs.Add((childTable, childColumns));
            }
        }

        foreach (var child in childRefs)
        {
            var predicates = child.Columns.Select(c => $"\"{c}\" is not null");
            var where = string.Join(" or ", predicates);
            var existsSql = $"select exists(select 1 from \"{child.Table}\" where {where} limit 1)";
            await using var cmd = new NpgsqlCommand(existsSql, conn);
            var existsObj = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (existsObj is bool b && b)
            {
                return true;
            }
        }

        return false;
    }

    public async Task<bool> ColumnHasInboundReferencesAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"select
                kcu.table_name as child_table,
                array_agg(kcu.column_name order by kcu.ordinal_position) as child_columns
            from information_schema.referential_constraints rc
            join information_schema.key_column_usage kcu on rc.constraint_name = kcu.constraint_name and rc.constraint_schema = kcu.constraint_schema
            join information_schema.constraint_column_usage ccu on rc.constraint_name = ccu.constraint_name and rc.constraint_schema = ccu.constraint_schema
            where ccu.table_schema = 'public' and kcu.table_schema = 'public' and ccu.table_name = @table and ccu.column_name = @column
            group by kcu.table_name";

        var childRefs = new List<(string Table, string[] Columns)>();

        await using (var cmd = new NpgsqlCommand(sql, conn))
        {
            cmd.Parameters.AddWithValue("table", tableName);
            cmd.Parameters.AddWithValue("column", columnName);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var childTable = reader.GetString(reader.GetOrdinal("child_table"));
                var childColumns = reader.GetFieldValue<string[]>(reader.GetOrdinal("child_columns"));
                childRefs.Add((childTable, childColumns));
            }
        }

        foreach (var child in childRefs)
        {
            var predicates = child.Columns.Select(c => $"\"{c}\" is not null");
            var where = string.Join(" or ", predicates);
            var existsSql = $"select exists(select 1 from \"{child.Table}\" where {where} limit 1)";
            await using var cmd = new NpgsqlCommand(existsSql, conn);
            var existsObj = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (existsObj is bool b && b)
            {
                return true;
            }
        }

        return false;
    }

    public async Task<bool> TableHasInboundForeignKeysAsync(string tableName, CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"select 1
            from information_schema.referential_constraints rc
            join information_schema.constraint_column_usage ccu on rc.constraint_name = ccu.constraint_name and rc.constraint_schema = ccu.constraint_schema
            where ccu.table_schema = 'public' and ccu.table_name = @table
            limit 1";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("table", tableName);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result != null;
    }

    public async Task<bool> ColumnHasInboundForeignKeysAsync(string tableName, string columnName, CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"select 1
            from information_schema.referential_constraints rc
            join information_schema.constraint_column_usage ccu on rc.constraint_name = ccu.constraint_name and rc.constraint_schema = ccu.constraint_schema
            where ccu.table_schema = 'public' and ccu.table_name = @table and ccu.column_name = @column
            limit 1";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("table", tableName);
        cmd.Parameters.AddWithValue("column", columnName);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result != null;
    }
}
