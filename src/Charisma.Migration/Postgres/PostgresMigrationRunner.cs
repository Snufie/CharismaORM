using System;
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

        if (plan.HasUnexecutable)
        {
            throw new InvalidOperationException("Unexecutable changes detected. Resolve schema or use force reset.");
        }

        if (plan.HasWarnings && !options.AllowDataLoss)
        {
            throw new InvalidOperationException("Data loss warnings present. Re-run with allowDataLoss=true to apply.");
        }

        if (plan.HasDestructiveChanges && !options.AllowDestructive)
        {
            throw new InvalidOperationException("Destructive changes detected. Re-run with allowDestructive=true to apply.");
        }

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        foreach (var step in plan.Steps)
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

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
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
