# Runtime Client

The runtime composes your generated client with provider-specific execution logic.

## Core Types

- `CharismaRuntime` (`src/Charisma.Runtime/CharismaRuntime.cs`)
- `CharismaRuntimeOptions` (`src/Charisma.Runtime/CharismaRuntimeOptions.cs`)
- `ProviderOptions` (`src/Charisma.Runtime/ProviderOptions.cs`)

## Minimal Setup

```csharp
var options = new CharismaRuntimeOptions
{
    ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=postgres",
    RootNamespace = "MyApp.Generated",
    Provider = ProviderOptions.PostgreSQL
};

using var client = new CharismaClient(options);
```

## Required vs Optional Options

Required (current default path):

- `ConnectionString` (unless custom `ConnectionProvider` is supplied)
- `RootNamespace`

Optional:

- `ConnectionProvider`
- `GeneratedAssembly`
- `MetadataRegistry`
- `ModelTypeResolver`
- `PreserveIdentifierCasing`
- `MaxNestingDepth`
- `GlobalOmit`

## Runtime Internals

At construction time, runtime:

- validates options
- chooses provider executor (PostgreSQL currently)
- wires metadata/type resolution hooks

Generated delegates then call executor methods via query model objects.

## Disposal Behavior

`CharismaRuntime` disposes:

- executor resources
- owned connection provider resources (if runtime created the provider)

This includes sync/async disposal paths.

## Global Omit and Casing

- `GlobalOmit` lets you define default omitted fields by model.
- `PreserveIdentifierCasing` controls identifier folding behavior in SQL planning.

## Common Misconfiguration Issues

- `RootNamespace` mismatch with generated assembly namespace
- wrong connection string source
- missing generated assembly when not using entry assembly defaults

## Startup Migration (ASP.NET)

You can now run migrations manually during app startup before `app.Run()`.

One-liner path (recommended):

```csharp
using (var client = new CharismaClient(new CharismaRuntimeOptions
{
    ConnectionString = builder.Configuration.GetConnectionString("Default")!,
    RootNamespace = "MyApp.Generated",
    Provider = ProviderOptions.PostgreSQL
}))
{
    await client.MigrateAsync("schema.charisma");
}
```

Advanced path (explicit parsed schema + safety options):

```csharp
using Charisma.Migration.Postgres;
using Charisma.Parser;
using MyApp.Generated;

var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

var schemaText = await File.ReadAllTextAsync("schema.charisma");
var schema = new RoslynSchemaParser().Parse(schemaText);

using (var client = new CharismaClient(new CharismaRuntimeOptions
{
    ConnectionString = builder.Configuration.GetConnectionString("Default")!,
    RootNamespace = "MyApp.Generated",
    Provider = ProviderOptions.PostgreSQL
}))
{
    await client.MigrateAsync(
        schema,
        new PostgresMigrationOptions(allowDestructive: false, allowDataLoss: false));
}

app.Run();
```

Notes:

- This keeps migration explicit and deterministic at startup.
- For production, keep `allowDestructive` and `allowDataLoss` false unless intentionally performing risky changes.

## How to Debug Setup Problems

Check in order:

1. generation namespace and output path
2. runtime options
3. assembly loading context
4. planner/executor exceptions and SQLSTATE mapping
