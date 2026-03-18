# Quickstart

This quickstart is for first-time users who want to go from zero to a working query in a few minutes.

## 1. Prerequisites

- .NET 8 SDK
- PostgreSQL
- A repository checkout of this project

## 2. Build the Solution

```bash
dotnet build Charisma.sln
```

## 3. Create or Reuse a Schema File

Use `schema.charisma` at the repository root or create your own minimal file.

Example minimal schema (datasource/generator blocks optional):

```charisma
model Robot {
  id        Id       @id @default(uuid())
  name      String
  createdAt DateTime @default(now())
}
```

You can omit `datasource` and `generator` blocks in your schema. Instead, set them via runtime options in your application (preferred for ASP.NET and DI scenarios).

---

## 4. Generate the Typed Client

```bash
charisma generate schema.charisma ./Generated --root-namespace MyApp.Generated
```

What this does:

- Parses and validates your schema
- Produces generated C# files (`Models`, `Delegates`, `Args`, `Filters`, and more)
- Embeds schema hash and generator metadata in output headers

## 5. Sync Database Schema

If your DB is empty or outdated, run:

```bash
charisma db push schema.charisma --connection "Host=localhost;Database=mydb;Username=postgres;Password=postgres"
```

Tip:

- Use `--plan-only` first if you want to preview steps.

## 6. Use the Generated Client in C#

```csharp
using MyApp.Generated;
using Charisma.Runtime;

var options = new CharismaRuntimeOptions
{
  ConnectionString = Environment.GetEnvironmentVariable("DATABASE_URL")!,
  RootNamespace = "MyApp.Generated",
  Provider = ProviderOptions.PostgreSQL
};

using var client = new CharismaClient(options);

var created = await client.Robot.CreateAsync(new RobotCreateArgs
{
  Data = new RobotCreateInput
  {
    Id = Guid.NewGuid(),
    Name = "R-001"
  }
});

var all = await client.Robot.FindManyAsync();
```

---

## 6b. ASP.NET Dependency Injection Example

You can register CharismaClient with DI in ASP.NET apps, using runtime options for configuration:

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

// Usage in controller/service:
public class RobotController : ControllerBase
{
  private readonly CharismaClient _client;
  public RobotController(CharismaClient client) => _client = client;

  // ... use _client.Robot ...
}
```

## 7. Common First Errors

- Schema parse error:
  - Fix DSL syntax and rerun `charisma generate`.
- Connection error:
  - Validate your connection string and PostgreSQL availability.
- Root namespace mismatch:
  - Ensure `RootNamespace` in runtime options matches generation namespace.

## 8. Next Steps

- Continue with [First Steps](first-steps.md) for a structured learning path.
- Read [Schema DSL](../core/schema-dsl.md) to model real relations and constraints.
- Read [Querying Data](../core/queries.md) for filtering, pagination, and projection.
