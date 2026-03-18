# Runtime Client

The runtime composes your generated client with provider-specific execution logic and exposes helpers for migrations, transactions and programmatic execution.

## Core types

- `CharismaRuntime` (`src/Charisma.Runtime/CharismaRuntime.cs`)
- `CharismaRuntimeOptions` (`src/Charisma.Runtime/CharismaRuntimeOptions.cs`)
- `ProviderOptions` (`src/Charisma.Runtime/ProviderOptions.cs`)

## Minimal construction

```csharp
var options = new CharismaRuntimeOptions
{
  ConnectionString = "Host=localhost;Database=mydb;Username=postgres;Password=postgres",
  RootNamespace = "MyApp.Generated",
  Provider = ProviderOptions.PostgreSQL
};

using var client = new CharismaClient(options);
```

### Required vs optional options

- Required: `ConnectionString` (unless you provide a custom `ConnectionProvider`), `RootNamespace`.
- Optional: `ConnectionProvider`, `GeneratedAssembly`, `MetadataRegistry`, `ModelTypeResolver`, `PreserveIdentifierCasing`, `MaxNestingDepth`, `GlobalOmit`.

## Dependency injection (ASP.NET)

Recommended pattern: register options and `CharismaClient` so you can inject `CharismaClient` into controllers or services:

```csharp
builder.Services.AddSingleton(new CharismaRuntimeOptions {
  ConnectionString = builder.Configuration.GetConnectionString("Default"),
  RootNamespace = "MyApp.Generated",
  Provider = ProviderOptions.PostgreSQL
});

builder.Services.AddScoped(sp => new CharismaClient(sp.GetRequiredService<CharismaRuntimeOptions>()));
```

Notes:

- Register `CharismaClient` as `Scoped` to match typical DbContext lifetimes in web apps.
- If you prefer a single shared client, register as `Singleton`, but ensure connection provider is thread-safe.

## Startup migration patterns

Prefer an explicit controlled migration step at startup. The runtime provides a `MigrateAsync()` helper that can run migrations using `schema.charisma` discovered in the app working directory by default:

```csharp
// non-blocking safe startup example in Program.cs
using var scope = app.Services.CreateScope();
var client = scope.ServiceProvider.GetRequiredService<CharismaClient>();
// await client.MigrateAsync(); // runs using schema.charisma in working dir

// Or run with a short timeout to avoid delaying app start:
var migrateTask = client.MigrateAsync();
await Task.WhenAny(migrateTask, Task.Delay(TimeSpan.FromSeconds(15)));
```

If you require deterministic control over the schema used for migration (for example, using a specific schema file embedded in your release pipeline), pass an explicit schema path when calling the lower-level migration entrypoint (the runtime also supports programmatic schema parsing via `Charisma.Parser`).

## Disposal and lifetimes

`CharismaRuntime` disposes executor resources and any owned `ConnectionProvider`. The runtime supports both sync and async disposal patterns; when used in ASP.NET, prefer `Scoped` registration and rely on the DI container to dispose instances at the end of scope.

## Transactions

Use `client.TransactionAsync(async trx => { ... })` to run a transactional block that can contain arbitrary C# logic and multiple delegate calls. The runtime ensures atomicity and automatic rollback on exceptions; manual rollback APIs are also available on the transaction context.

## Debugging and common misconfigurations

Check in this order when debugging startup/runtime problems:

1. generation namespace matches `RootNamespace` and the generated assembly is loaded.
2. `ConnectionString` and `ConnectionProvider` are correct and reachable.
3. `Provider` is set to the provider you generated code for (Postgres currently).
4. planner/executor exceptions — inspect SQL and SQLSTATE codes.

## Tips

- Keep migrations explicit in CI pipelines and only run `MigrateAsync` in production under controlled conditions.
- When testing locally, use a disposable test database or transactional test harness to avoid interfering with shared state.
