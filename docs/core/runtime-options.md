# Charisma Runtime Options

CharismaRuntimeOptions configures the Charisma ORM runtime for your application. You can set connection strings, provider, root namespace, and advanced options.

## Example: Basic Usage

```csharp
using Charisma.Runtime;

var options = new CharismaRuntimeOptions
{
    ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=postgres",
    RootNamespace = "MyApp.Generated",
    Provider = ProviderOptions.PostgreSQL
};

using var client = new CharismaClient(options);
```

## Example: Dependency Injection (ASP.NET)

```csharp
using Charisma.Runtime;
using Microsoft.Extensions.DependencyInjection;

builder.Services.AddSingleton<CharismaRuntimeOptions>(sp => new CharismaRuntimeOptions
{
    ConnectionString = builder.Configuration.GetConnectionString("DefaultConnection"),
    RootNamespace = "MyApp.Generated",
    Provider = ProviderOptions.PostgreSQL
});

builder.Services.AddScoped<CharismaClient>(sp =>
    new CharismaClient(sp.GetRequiredService<CharismaRuntimeOptions>()));
```

## Advanced: Manual Migration with DI

You can manually run migrations at startup using DI:

```csharp
using Charisma.Runtime;
using Charisma.Schema;

public class MigrationService
{
    private readonly CharismaClient _client;
    private readonly CharismaRuntimeOptions _options;
    public MigrationService(CharismaClient client, CharismaRuntimeOptions options)
    {
        _client = client;
        _options = options;
    }

    public async Task MigrateAsync(CharismaSchema schema)
    {
        await _client.Runtime.MigrateAsync(schema);
    }
}
```

## Option Reference

- `ConnectionString`: Database connection string
- `Provider`: Database provider (PostgreSQL, ...)
- `RootNamespace`: Namespace of generated code
- `GeneratedAssembly`: Optional assembly override
- `MetadataRegistry`: Optional metadata override
- `ModelTypeResolver`: Custom model type resolver
- `PreserveIdentifierCasing`: Use mixed-case identifiers
- `MaxNestingDepth`: Maximum relation depth
