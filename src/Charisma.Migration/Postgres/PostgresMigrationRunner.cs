using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Charisma.Migration;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Executes planned migration steps against Postgres with safety checks.
/// </summary>
public sealed class PostgresMigrationRunner
{
    // Stable, project-specific advisory lock key to serialize concurrent migration runners per database.
    private const long AdvisoryLockKey = 3203321129043893251L;
    private const string MigrationHistoryTable = "__charisma_migrations";

    private readonly string _connectionString;
    private readonly DropSafetyChecker _safetyChecker;

    public PostgresMigrationRunner(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        _safetyChecker = new DropSafetyChecker(_connectionString);
    }

    public async Task ExecuteAsync(MigrationPlan plan, PostgresMigrationOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(options);

        // Never execute steps against the internal bookkeeping table.
        var effectivePlan = new MigrationPlan(
            plan.Steps.Where(step => !TouchesMigrationHistoryTable(step)).ToArray(),
            plan.Warnings,
            plan.Unexecutable);

        if (effectivePlan.HasUnexecutable)
        {
            throw new InvalidOperationException("Unexecutable changes detected. Resolve schema or use force reset.");
        }

        if (effectivePlan.HasWarnings && !options.AllowDataLoss)
        {
            throw new InvalidOperationException("Data loss warnings present. Re-run with allowDataLoss=true to apply.");
        }

        if (effectivePlan.HasDestructiveChanges && !options.AllowDestructive)
        {
            throw new InvalidOperationException("Destructive changes detected. Re-run with allowDestructive=true to apply.");
        }

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await AcquireMigrationLockAsync(conn, tx, cancellationToken).ConfigureAwait(false);
        await EnsureMigrationHistoryTableAsync(conn, tx, cancellationToken).ConfigureAwait(false);

        var migrationKey = BuildMigrationKey(effectivePlan);
        if (await IsAlreadyAppliedAsync(conn, tx, migrationKey, cancellationToken).ConfigureAwait(false))
        {
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        foreach (var step in effectivePlan.Steps)
        {
            if (step.Sql is null) continue;

            if (step.IsDestructive && !options.AllowDestructive)
            {
                throw new InvalidOperationException($"Destructive step blocked: {step.Description}");
            }

            if (step.IsDestructive && !options.AllowDataLoss)
            {
                await EnsureSafeForDestructionAsync(step.Description, cancellationToken).ConfigureAwait(false);
            }

            await using var cmd = new NpgsqlCommand(step.Sql, conn, tx);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await InsertMigrationHistoryAsync(conn, tx, migrationKey, effectivePlan, cancellationToken).ConfigureAwait(false);

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static bool TouchesMigrationHistoryTable(MigrationStep step)
    {
        if (step.Sql is not null && step.Sql.Contains(MigrationHistoryTable, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return step.Description.Contains(MigrationHistoryTable, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Acquires a transaction-scoped advisory lock to prevent concurrent migration application.
    /// </summary>
    private static async Task AcquireMigrationLockAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand("select pg_advisory_xact_lock(@k)", conn, tx);
        cmd.Parameters.AddWithValue("k", AdvisoryLockKey);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Ensures the migration history table exists before any plan is applied.
    /// </summary>
    private static async Task EnsureMigrationHistoryTableAsync(NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken cancellationToken)
    {
        var sql = $@"
    create table if not exists ""{MigrationHistoryTable}"" (
    migration_key text primary key,
    applied_at timestamp with time zone not null default now(),
    step_count integer not null,
    plan_hash text not null
);";

        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Determines whether an equivalent migration plan has already been applied.
    /// </summary>
    private static async Task<bool> IsAlreadyAppliedAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string migrationKey, CancellationToken cancellationToken)
    {
        var sql = $"select exists(select 1 from \"{MigrationHistoryTable}\" where migration_key = @k)";
        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        cmd.Parameters.AddWithValue("k", migrationKey);
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value is bool exists && exists;
    }

    /// <summary>
    /// Persists migration bookkeeping after all steps execute successfully.
    /// </summary>
    private static async Task InsertMigrationHistoryAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string migrationKey, MigrationPlan plan, CancellationToken cancellationToken)
    {
        var planHash = ComputePlanHash(plan);
        var sql = $@"insert into ""{MigrationHistoryTable}"" (migration_key, step_count, plan_hash)
values (@k, @c, @h);";

        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        cmd.Parameters.AddWithValue("k", migrationKey);
        cmd.Parameters.AddWithValue("c", plan.Steps.Count);
        cmd.Parameters.AddWithValue("h", planHash);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Builds a deterministic key for the migration plan so reruns are idempotent.
    /// </summary>
    private static string BuildMigrationKey(MigrationPlan plan)
    {
        return $"auto:{ComputePlanHash(plan)}";
    }

    /// <summary>
    /// Computes a SHA-256 hash from all plan semantics that affect execution.
    /// </summary>
    private static string ComputePlanHash(MigrationPlan plan)
    {
        var sb = new StringBuilder();
        foreach (var step in plan.Steps)
        {
            sb.Append(step.Description).Append('|')
              .Append(step.IsDestructive).Append('|')
              .Append(step.Sql ?? string.Empty).Append('\n');
        }

        foreach (var warning in plan.Warnings)
        {
            sb.Append("warn:").Append(warning).Append('\n');
        }

        foreach (var blocked in plan.Unexecutable)
        {
            sb.Append("block:").Append(blocked).Append('\n');
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash);
    }

    private async Task EnsureSafeForDestructionAsync(string description, CancellationToken cancellationToken)
    {
        // Simple heuristic: look for "Drop table <name>" or "Drop column <table>.<col>" in description.
        if (description.StartsWith("Drop table ", StringComparison.OrdinalIgnoreCase))
        {
            var table = description.Substring("Drop table ".Length).Trim();
            var isEmpty = await _safetyChecker.IsTableEmptyAsync(table, cancellationToken).ConfigureAwait(false);
            if (!isEmpty)
            {
                throw new InvalidOperationException($"Table {table} is not empty; destructive drop blocked. Re-run with allowDataLoss=true.");
            }
        }
    }
}
