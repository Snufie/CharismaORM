# CharismaORM Documentation

CharismaORM is a schema-first ORM stack for .NET that generates a strongly typed client from `schema.charisma`, then executes typed query models through a PostgreSQL query engine.

This docs set is intentionally implementation-driven, based on the code in this repository and the existing automated tests.

## Start Here

If this is your first time with CharismaORM, read in this order:

1. [Quickstart](getting-started/quickstart.md)
2. [First Steps](getting-started/first-steps.md)
3. [Mental Model](getting-started/mental-model.md)

## Main Guides

- [Core Overview](core/index.md)
  - Schema DSL: [Schema DSL](core/schema-dsl.md)
  - Code Generation: [Code Generation](core/generation.md)
  - Runtime Client: [Runtime Client](core/runtime-client.md)
  - Runtime Options: [Runtime Options](core/runtime-options.md)
  - Delegates & Usage: [Delegates](core/delegates.md)
  - Sugar Methods: [Sugar Methods](core/sugar-methods.md)
  - Optional Lists: [Optional Lists](core/optional-lists.md)
  - JSON Filtering: [JSON Filtering](core/json-filtering.md)
  - Querying Data: [Querying Data](core/queries.md)
  - Transactions: [Transactions](core/transactions.md)
  - Advanced Transactions: [Transactions: Advanced](core/transactions-advanced.md)
  - JSON Fields: [JSON Fields](core/json.md)

## Operations

- [CLI Reference](operations/cli.md)
- [CLI Completion Roadmap](operations/cli-roadmap.md)
- [Migrations and Introspection](operations/migrations.md)
- [Release Gates](operations/release-gates.md)

## Architecture

- [Architecture Overview](architecture/overview.md)
- [Example ERD](architecture/erd.md)

## Reference

- [Capability Matrix](reference/capabilities-matrix.md)
- [Error Reference](reference/error-reference.md)
- [Project Map](reference/project-map.md)
- [Limitations](reference/limitations.md)

## Current Status Snapshot

Implemented now:

- PostgreSQL provider path
- CLI commands: `generate`, `db pull`, `db push`, `migrate`
- Typed query operations: reads, writes, aggregate, groupBy
- Projection system with strict exclusivity (`Select`/`Include`/`Omit`) and operation-specific executor limits
- JSON filter support in planner/executor path
- Transaction support in generated client and query engine
- Migration planning and execution safety model
