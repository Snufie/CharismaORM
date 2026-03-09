using System;

namespace Charisma.Migration.Introspection.Push.Postgres;

public sealed class PostgresPushOptions
{
    public string ConnectionString { get; }

    public PostgresPushOptions(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string is required", nameof(connectionString));
        }

        ConnectionString = connectionString;
    }
}
