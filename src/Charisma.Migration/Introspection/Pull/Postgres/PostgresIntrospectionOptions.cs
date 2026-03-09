using System;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Configuration for Postgres schema introspection and emitted datasource/generator blocks.
/// </summary>
public sealed class PostgresIntrospectionOptions
{
    public string ConnectionString { get; }
    public string DatasourceName { get; }
    public string GeneratorName { get; }
    public string GeneratorProvider { get; }
    public string? GeneratorOutput { get; }

    public PostgresIntrospectionOptions(
        string connectionString,
        string datasourceName = "db",
        string generatorName = "client",
        string generatorProvider = "charisma-generator",
        string? generatorOutput = "./Generated")
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string is required", nameof(connectionString));
        }

        ConnectionString = connectionString;
        DatasourceName = datasourceName ?? throw new ArgumentNullException(nameof(datasourceName));
        GeneratorName = generatorName ?? throw new ArgumentNullException(nameof(generatorName));
        GeneratorProvider = generatorProvider ?? throw new ArgumentNullException(nameof(generatorProvider));
        GeneratorOutput = generatorOutput;
    }
}
