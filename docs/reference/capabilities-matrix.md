# Capability Matrix

This matrix summarizes current repository capabilities.

## Platform and Provider

| Area                     | Status          | Notes                               |
| ------------------------ | --------------- | ----------------------------------- |
| Runtime Provider         | Implemented     | PostgreSQL path available           |
| Non-PostgreSQL Providers | Not implemented | Current stack is PostgreSQL-focused |

## CLI

| Command                | Status          | Notes                          |
| ---------------------- | --------------- | ------------------------------ |
| `charisma`             | Implemented     | prints usage/help and exits 0  |
| `charisma generate`    | Implemented     | schema parse + code generation |
| `charisma db pull`     | Implemented     | introspect DB to schema        |
| `charisma db push`     | Implemented     | plan + apply migration         |
| `charisma migrate ...` | Not implemented | routed but marked planned      |

## Query Operations

| Operation Group                         | Status      | Notes                    |
| --------------------------------------- | ----------- | ------------------------ |
| Reads (`FindUnique/First/Many`)         | Implemented | typed args and filters   |
| Writes (`Create/Update/Delete/Upsert`)  | Implemented | single + batch variants  |
| Aggregation (`Count/Aggregate/GroupBy`) | Implemented | planner/executor support |
| Transactions                            | Implemented | scope-based APIs         |

## Query Features

- Scalar filters: Implemented (equals/range/in/not operators)
- String filters: Implemented (contains/startsWith/endsWith + mode)
- Relation filters: Implemented (some/none/every/is/isNot)
- JSON filters: Implemented (path + array operations)
- Pagination: Implemented (skip/take/cursor)
- Distinct: Implemented (validated constraints)
- Select/include/omit: Partially implemented (mutually exclusive; include rejected for delete and bulk mutations)

## Schema and Generation

| Feature                            | Status      | Notes                             |
| ---------------------------------- | ----------- | --------------------------------- |
| Schema parse + semantic validation | Implemented | aggregate diagnostics             |
| Canonical normalization + hash     | Implemented | deterministic output basis        |
| Full generated client surface      | Implemented | models/delegates/args/filters/etc |

## Migrations

| Feature            | Status      | Notes                             |
| ------------------ | ----------- | --------------------------------- |
| Introspection pull | Implemented | tables/columns/keys/indexes/enums |
| Diff planning      | Implemented | warnings + unexecutable channels  |
| SQL apply runner   | Implemented | lock + history + idempotency      |
| Force reset        | Implemented | dev reset path                    |
