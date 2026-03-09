using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;
using Charisma.Migration.Introspection.Push.Postgres;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Postgres-specific planner; will compute diff and mark destructive steps for confirmation.
/// </summary>
public sealed class PostgresMigrationPlanner : IMigrationPlanner
{
    private readonly PostgresDiffPlanner _diffPlanner;
    private readonly PostgresMigrationOptions _options;

    public PostgresMigrationPlanner(PostgresIntrospectionOptions options, PostgresMigrationOptions? migrationOptions = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = migrationOptions ?? new PostgresMigrationOptions();
        var introspector = new PostgresDatabaseIntrospector(options.ConnectionString);
        var safetyChecker = new DropSafetyChecker(options.ConnectionString);
        _diffPlanner = new PostgresDiffPlanner(introspector, _options, safetyChecker);
    }

    public Task<MigrationPlan> PlanAsync(CharismaSchema schema, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        cancellationToken.ThrowIfCancellationRequested();
        return _diffPlanner.PlanAsync(schema, cancellationToken);
    }
}
