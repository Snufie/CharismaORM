# First Steps

This guide explains the most basic concepts for first-timers, in plain language.

## The 3 Core Things

CharismaORM revolves around three layers:

1. Schema (`schema.charisma`):
   - You describe models, relations, defaults, and constraints.
2. Generated Client (C#):
   - Charisma turns schema definitions into strongly typed classes and methods.
3. Runtime + Query Engine:
   - Generated delegate methods build query models and execute them against PostgreSQL.

## What You Actually Write

As an app developer, you mostly write:

- `schema.charisma`
- normal C# application code calling `client.ModelName.SomeMethodAsync(...)`

You do not manually write SQL for normal CRUD flows.

## Basic Workflow

1. Update schema.
2. Run `charisma generate`.
3. Run `charisma db push` (or `db pull` when database is your source).
4. Build and run your app.

## First Schema Concepts

- `model`: a table-like type
- scalar field: `String`, `Int`, `DateTime`, `Json`, etc.
- `@id`: primary key marker
- `@default(...)`: default value marker
- `@relation(...)`: relationship between models
- `@@unique` / `@@index`: model-level constraints

## First Query Concepts

- `FindManyAsync`: list records
- `FindUniqueAsync`: get one record by unique selector
- `CreateAsync`: insert one record
- `UpdateAsync`: update one record
- `DeleteAsync`: delete one record

## Projection Basics

A query can shape returned data using one of:

- `Select`
- `Include`
- `Omit`

Important:

- They are mutually exclusive in a single query args object.
- `Include` support is operation-specific in mutation paths; read operations are the safest default for include-heavy shapes.

## Transaction Basics

Use `client.TransactionAsync(...)` to execute multiple steps atomically.

If one step fails, rollback keeps the database consistent.

## Migration Basics

- `db pull`: read current DB structure into schema file.
- `db push`: compare schema vs DB and apply diff.

Safety behavior includes warnings and destructive/unexecutable checks.

## What to Learn Next

- [Mental Model](mental-model.md)
- [Schema DSL](../core/schema-dsl.md)
- [CLI Reference](../operations/cli.md)
