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

Example minimal schema:

```charisma
datasource db {
  provider = "postgresql"
  url = env("DATABASE_URL")
}

generator client {
  provider = "charisma-generator"
  output = "./Generated"
}

model Robot {
  id        Id       @id @default(uuid())
  name      String
  createdAt DateTime @default(now())
}
```

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
