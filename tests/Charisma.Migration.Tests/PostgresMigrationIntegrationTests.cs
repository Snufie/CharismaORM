using Charisma.Migration.Postgres;
using Charisma.Schema;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Charisma.Migration.Tests;

/// <summary>
/// End-to-end migration tests against a real PostgreSQL instance.
/// Set CHARISMA_RUN_PG_INTEGRATION=1 to enable these tests.
/// </summary>
public sealed class PostgresMigrationIntegrationTests : IAsyncLifetime
{
    private PostgreSqlContainer? _container;

    private static bool IntegrationEnabled =>
        string.Equals(Environment.GetEnvironmentVariable("CHARISMA_RUN_PG_INTEGRATION"), "1", StringComparison.Ordinal);

    public async Task InitializeAsync()
    {
        if (!IntegrationEnabled)
        {
            return;
        }

        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .WithDatabase("charisma_test")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();

        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (IntegrationEnabled && _container is not null)
        {
            await _container.DisposeAsync();
        }
    }

    [PostgresIntegrationFact]
    public async Task PlannerAndRunner_CreateTable_OnEmptyDatabase()
    {
        var connectionString = GetConnectionString();
        var resetter = new PostgresDatabaseResetter(connectionString);
        await resetter.ResetAsync();

        var desired = BuildRobotSchema(includeName: true);
        var planner = new PostgresMigrationPlanner(
            new PostgresIntrospectionOptions(connectionString),
            new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        var plan = await planner.PlanAsync(desired);

        Assert.Contains(plan.Steps, s => s.Description.Contains("Create table Robot", StringComparison.Ordinal));

        var runner = new PostgresMigrationRunner(connectionString);
        await runner.ExecuteAsync(plan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var tableCmd = new NpgsqlCommand("select exists(select 1 from information_schema.tables where table_schema = 'public' and table_name = 'Robot')", conn);
        var tableExists = (bool)(await tableCmd.ExecuteScalarAsync() ?? false);
        Assert.True(tableExists);

        await using var columnCmd = new NpgsqlCommand("select exists(select 1 from information_schema.columns where table_schema = 'public' and table_name = 'Robot' and column_name = 'Name')", conn);
        var columnExists = (bool)(await columnCmd.ExecuteScalarAsync() ?? false);
        Assert.True(columnExists);
    }

    [PostgresIntegrationFact]
    public async Task Runner_BlocksDropWhenDataLossNotAllowed_ThenSucceedsWhenAllowed()
    {
        var connectionString = GetConnectionString();
        var resetter = new PostgresDatabaseResetter(connectionString);
        await resetter.ResetAsync();

        var planner = new PostgresMigrationPlanner(
            new PostgresIntrospectionOptions(connectionString),
            new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        var initialPlan = await planner.PlanAsync(BuildRobotSchema(includeName: true));
        var runner = new PostgresMigrationRunner(connectionString);
        await runner.ExecuteAsync(initialPlan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        await InsertRobotRowAsync(connectionString);

        var dropPlan = await planner.PlanAsync(EmptySchema());
        Assert.True(dropPlan.HasDestructiveChanges);
        Assert.True(dropPlan.HasWarnings);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            runner.ExecuteAsync(dropPlan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: false, allowRenames: true)));

        await runner.ExecuteAsync(dropPlan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var tableCmd = new NpgsqlCommand("select exists(select 1 from information_schema.tables where table_schema = 'public' and table_name = 'Robot')", conn);
        var tableExists = (bool)(await tableCmd.ExecuteScalarAsync() ?? false);
        Assert.False(tableExists);
    }

    [PostgresIntegrationFact]
    public async Task Runner_RecordsMigrationHistory_AndSkipsEquivalentReapply()
    {
        var connectionString = GetConnectionString();
        var resetter = new PostgresDatabaseResetter(connectionString);
        await resetter.ResetAsync();

        var desired = BuildRobotSchema(includeName: true);
        var planner = new PostgresMigrationPlanner(
            new PostgresIntrospectionOptions(connectionString),
            new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        var plan = await planner.PlanAsync(desired);
        var runner = new PostgresMigrationRunner(connectionString);
        await runner.ExecuteAsync(plan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));
        await runner.ExecuteAsync(plan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var historyCmd = new NpgsqlCommand("select count(*) from \"__charisma_migrations\"", conn);
        var historyCount = Convert.ToInt32(await historyCmd.ExecuteScalarAsync() ?? 0);
        Assert.Equal(1, historyCount);
    }

    [PostgresIntegrationFact]
    public async Task Runner_IgnoresPlanStepsThatTouchInternalMigrationHistoryTable()
    {
        var connectionString = GetConnectionString();
        var resetter = new PostgresDatabaseResetter(connectionString);
        await resetter.ResetAsync();

        var planner = new PostgresMigrationPlanner(
            new PostgresIntrospectionOptions(connectionString),
            new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        var runner = new PostgresMigrationRunner(connectionString);
        var seedPlan = await planner.PlanAsync(BuildRobotSchema(includeName: true));
        await runner.ExecuteAsync(seedPlan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        var emptyPlan = await planner.PlanAsync(EmptySchema());
        Assert.Contains(emptyPlan.Steps, s =>
            (s.Sql?.Contains("__charisma_migrations", StringComparison.OrdinalIgnoreCase) ?? false)
            || s.Description.Contains("__charisma_migrations", StringComparison.OrdinalIgnoreCase));

        await runner.ExecuteAsync(emptyPlan, new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true, allowRenames: true));

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var historyExistsCmd = new NpgsqlCommand("select to_regclass('public.\"__charisma_migrations\"') is not null", conn);
        var historyExists = (bool)(await historyExistsCmd.ExecuteScalarAsync() ?? false);
        Assert.True(historyExists);
    }

    private static async Task InsertRobotRowAsync(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();

        await using var cmd = new NpgsqlCommand("insert into \"Robot\" (\"RobotID\", \"Name\") values (@id, @name)", conn);
        cmd.Parameters.AddWithValue("id", Guid.NewGuid());
        cmd.Parameters.AddWithValue("name", "r1");
        await cmd.ExecuteNonQueryAsync();
    }

    private static CharismaSchema BuildRobotSchema(bool includeName)
    {
        var fields = new List<FieldDefinition>
        {
            new ScalarFieldDefinition("RobotID", "Id", false, false, new[] { "@id" }, isId: true)
        };

        if (includeName)
        {
            fields.Add(new ScalarFieldDefinition("Name", "String", false, false, Array.Empty<string>()));
        }

        return new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["Robot"] = new ModelDefinition(
                    "Robot",
                    fields,
                    Array.Empty<string>(),
                    primaryKey: new PrimaryKeyDefinition(new[] { "RobotID" }))
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());
    }

    private static CharismaSchema EmptySchema()
    {
        return new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal),
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());
    }

    private string GetConnectionString()
    {
        return _container?.GetConnectionString()
            ?? throw new InvalidOperationException("PostgreSQL container was not initialized for integration testing.");
    }
}

internal sealed class PostgresIntegrationFactAttribute : FactAttribute
{
    public PostgresIntegrationFactAttribute()
    {
        var enabled = string.Equals(Environment.GetEnvironmentVariable("CHARISMA_RUN_PG_INTEGRATION"), "1", StringComparison.Ordinal);
        if (!enabled)
        {
            Skip = "Set CHARISMA_RUN_PG_INTEGRATION=1 to run PostgreSQL integration tests.";
            return;
        }

        try
        {
            _ = new PostgreSqlBuilder()
                .WithImage("postgres:16-alpine")
                .WithDatabase("charisma_test")
                .WithUsername("postgres")
                .WithPassword("postgres")
                .Build();
        }
        catch (Exception ex)
        {
            // Explicit skip ensures these tests do not report as passed when infrastructure is unavailable.
            Skip = $"PostgreSQL integration prerequisites unavailable: {ex.Message}";
        }
    }
}
