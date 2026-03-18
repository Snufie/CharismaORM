# CharismaORM

Schema-first ORM tooling for .NET with generated, strongly typed APIs and a PostgreSQL query engine.

## Documentation

- Docs index: [docs/index.md](docs/index.md)
- MkDocs config: [mkdocs.yml](mkdocs.yml)
- CLI docs: [docs/operations/cli.md](docs/operations/cli.md)
- Migrations docs: [docs/operations/migrations.md](docs/operations/migrations.md)

If GitHub Pages is enabled for this repository, docs can be published from `docs/` using the workflow in `.github/workflows/docs-pages.yml`.

## Quickstart

```bash
dotnet build Charisma.sln
charisma generate schema.charisma ./Generated --root-namespace MyApp.Generated
```

## CLI

### `charisma`

Prints usage/help and exits successfully.

### `charisma generate`

```bash
charisma generate [schemaPath] [outputPath] [--root-namespace <ns>]
```

### `charisma db pull`

```bash
charisma db pull [schemaPath] [--connection <conn>] [--force]
```

### `charisma db push`

```bash
charisma db push [schemaPath] [--connection <conn>] [--force-reset] [--accept-data-loss] [--yes] [--emit-sql <file>] [--plan-only]
```

Examples:

```bash
# Preview only
charisma db push schema.charisma --plan-only --emit-sql ./plan.sql --yes --accept-data-loss

# Apply plan
charisma db push schema.charisma --connection "Host=localhost;Database=mydb;Username=postgres;Password=postgres"

# Force reset and rebuild schema (development only)
charisma db push schema.charisma --force-reset --connection "Host=localhost;Database=mydb;Username=postgres;Password=postgres"
```

Behavior summary:

- Computes a migration plan from your schema to the current PostgreSQL state.
- Blocks unexecutable changes and explains why.
- Requires explicit acceptance for data-loss paths.
- Supports SQL export via `--emit-sql`.

## Runtime example

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
var rows = await client.Robot.FindManyAsync();
```

## Status snapshot

Implemented:

- `charisma`, `generate`, `db pull`, `db push`
- typed read/write/aggregate/groupBy APIs
- migration planner + runner
- startup migration helper (`await client.MigrateAsync("schema.charisma")`)

Planned:

- `charisma migrate ...` command family
