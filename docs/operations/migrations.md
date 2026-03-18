# Migrations and Introspection

CharismaORM includes PostgreSQL-focused migration planning and schema introspection.

## Core Types

- `MigrationPlan` (`src/Charisma.Migration/MigrationPlan.cs`)
- `PostgresMigrationPlanner` (`src/Charisma.Migration/Postgres/PostgresMigrationPlanner.cs`)
- `PostgresMigrationRunner` (`src/Charisma.Migration/Postgres/PostgresMigrationRunner.cs`)
- `PostgresSchemaIntrospector` (`src/Charisma.Migration/Introspection/Pull/Postgres/PostgresSchemaIntrospector.cs`)
- `PostgresSchemaPusher` (`src/Charisma.Migration/Introspection/Push/Postgres/PostgresSchemaPusher.cs`)

## Migration Plan Shape

A plan contains:

- ordered migration steps (`description`, `isDestructive`, `sql`)
- warnings
- unexecutable reasons

Helper flags:

- `HasDestructiveChanges`
- `HasWarnings`
- `HasUnexecutable`

## Planning Workflow

1. desired schema from parser
2. current schema from DB introspection
3. diff planner computes steps
4. warnings/unexecutable channels populated as needed

## Runner Safety Workflow

`PostgresMigrationRunner` enforces:

- unexecutable blocking
- warning/data-loss gates
- destructive gates
- advisory lock for serialization
- migration history table tracking
- plan-hash idempotency checks

## `db pull` Introspection Coverage

Introspection covers:

- tables and columns
- primary keys
- foreign keys
- unique constraints
- indexes
- enums

Writer behavior supports no-op skipping and overwrite handling.

## `db push` Push Coverage

Push supports:

- enum type create/drop
- table create/drop
- column add/alter/drop
- PK/unique/index/FK operations
- default and nullability updates
- type change handling with warnings/unexecutable classification

## Force Reset

`--force-reset` path:

- reset public schema
- reapply desired structure

Use this carefully on development environments.

## Security and Escaping

Migration SQL builder includes:

- identifier quoting
- embedded quote escaping
- SQL literal escaping for dynamic statements

## Integration Testing

See `tests/Charisma.Migration.Tests/PostgresMigrationIntegrationTests.cs`.

Integration tests are env-gated via `CHARISMA_RUN_PG_INTEGRATION=1`.
