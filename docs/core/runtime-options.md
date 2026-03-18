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

## Option reference (common)

- `ConnectionString` (string) — full DB connection string. If omitted, runtime will attempt `CHARISMA_CONNECTION_STRING` then `DATABASE_URL` env vars.
- `Provider` (ProviderOptions) — which provider executor to use (PostgreSQL currently supported).
- `RootNamespace` (string) — namespace the generator emitted types into (required unless `GeneratedAssembly` provided).
- `GeneratedAssembly` (Assembly?) — optional pre-loaded assembly that contains generated types; when supplied the runtime will use it instead of loading by convention.
- `ConnectionProvider` (IConnectionProvider) — advanced: supply a custom connection provider (for connection pooling/testing).
- `MetadataRegistry` (IMetadataRegistry) — optional override for model metadata.
- `ModelTypeResolver` (Func&lt;ModelMetadata, Type&gt;) — custom mapping from model metadata to CLR types.
- `PreserveIdentifierCasing` (bool) — preserve mixed-case identifiers when building SQL.
- `MaxNestingDepth` (int) — query/include nesting safety cap.
- `GlobalOmit` (dictionary) — global projection omissions by model.

## Environment variable defaults

- `CHARISMA_CONNECTION_STRING` — preferred env var the runtime will read if `ConnectionString` not provided.
- `DATABASE_URL` — fallback env var commonly used by hosting platforms.

## Advanced examples

Provide an explicit generated assembly (useful when runtime cannot resolve by name):

```csharp
var assembly = Assembly.Load("MyApp.Generated");
var options = new CharismaRuntimeOptions { GeneratedAssembly = assembly, Provider = ProviderOptions.PostgreSQL };
using var client = new CharismaClient(options);
```

Custom connection provider (test or special pool):

```csharp
var provider = new MyTestConnectionProvider();
var options = new CharismaRuntimeOptions { ConnectionProvider = provider, RootNamespace = "MyApp.Generated", Provider = ProviderOptions.PostgreSQL };
```

## Migration helper behavior

- `CharismaClient.MigrateAsync()` — convenience helper that will attempt to read `schema.charisma` from the current working directory when no schema path is supplied. Callers do not need to provide a `CharismaSchema` object.
- For deterministic CI/pipeline migrations pass an explicit schema path or parse the schema with `RoslynSchemaParser` and invoke the lower-level migration APIs.

## Testing guidance

- For unit tests prefer injecting a lightweight `ConnectionProvider` that targets an ephemeral test DB or uses embedded containers. Avoid running `MigrateAsync()` in unit tests; prefer integration tests that exercise migration behavior in isolation.

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

You can run migrations at startup using DI. Prefer the `CharismaClient.MigrateAsync` helper which loads `schema.charisma` from the app working directory by default:

```csharp
using Charisma.Runtime;

public class MigrationService
{
    private readonly CharismaClient _client;
    public MigrationService(CharismaClient client)
    {
        _client = client;
    }

    public async Task MigrateAsync(string? schemaPath = null)
    {
        // schemaPath defaults to "schema.charisma" when null
        await _client.MigrateAsync(schemaPath);
    }
}
```

If you need lower-level control you can also call `_client.Runtime.MigrateAsync()` which accepts an optional schema path.

## Option Reference

- `ConnectionString`: Database connection string
- `Provider`: Database provider (PostgreSQL, ...)
- `RootNamespace`: Namespace of generated code
- `GeneratedAssembly`: Optional assembly override
- `MetadataRegistry`: Optional metadata override
- `ModelTypeResolver`: Custom model type resolver
- `PreserveIdentifierCasing`: Use mixed-case identifiers
- `MaxNestingDepth`: Maximum relation depth
