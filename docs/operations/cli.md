# CLI Reference

CLI entry point: `src/Charisma.Client/Program.cs`

## Command Overview

```text
charisma [--help|-h]
charisma generate [schemaPath] [outputPath] [--root-namespace <ns>]
charisma db pull [schemaPath] [--connection <conn>] [--force]
charisma db push [schemaPath] [--connection <conn>] [--force-reset] [--accept-data-loss] [--yes] [--emit-sql <file>] [--plan-only]
charisma migrate ...   (not implemented yet)
```

## Default Help Behavior

- Running `charisma` without arguments prints usage/help and exits successfully.
- `charisma --help`, `charisma -h`, and `charisma help` are aliases for the same output.

## `generate`

Purpose:

- parse and validate schema
- generate typed client code

Defaults:

- schema: `schema.charisma` in current directory
- output: generator config output or `./Generated`
- root namespace: generator config or `<cwd>.Generated`

## `db pull`

Purpose:

- introspect PostgreSQL and produce/update schema file

Connection resolution order:

1. explicit `--connection`
2. `CHARISMA_CONNECTION_STRING` / `DATABASE_URL`
3. datasource URL (including `env(...)`)

Write modes:

- full rewrite on missing file / empty schema / `--force`
- datasource-block update mode otherwise

## `db push`

Purpose:

- compute migration plan and apply to PostgreSQL

Important flags:

- `--plan-only`: preview only
- `--emit-sql <file>`: write generated SQL steps to file
- `--accept-data-loss`: allow warning path
- `--yes`: non-interactive mode
- `--force-reset`: drop/recreate schema and push fresh

Behavior:

- blocks unexecutable plans
- warns/aborts for data-loss risk unless explicitly accepted
- executes with migration runner safety checks

## Usage Examples

Generate:

```bash
charisma generate schema.charisma ./Generated --root-namespace MyApp.Generated
```

Preview push:

```bash
charisma db push schema.charisma --plan-only --emit-sql ./plan.sql --yes --accept-data-loss
```

Force reset push:

```bash
charisma db push schema.charisma --force-reset --connection "Host=localhost;Database=mydb;Username=postgres;Password=postgres"
```

## Related Operational Docs

- [CLI Completion Roadmap](cli-roadmap.md)
- [Release Gates](release-gates.md)
