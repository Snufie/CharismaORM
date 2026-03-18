# Project Map

This page maps each project to its runtime role.

## Solution Structure

- `src/Charisma.Schema`
- `src/Charisma.Parser`
- `src/Charisma.Generator`
- `src/Charisma.QueryEngine`
- `src/Charisma.Runtime`
- `src/Charisma.Migration`
- `src/Charisma.Client`
- `src/Charisma.All`

## Detailed Responsibilities

### `Charisma.Schema`

- IR object model (`CharismaSchema`, model/field/enum definitions)
- normalization and hashing (`SchemaNormalizer`, schema hash)

### `Charisma.Parser`

- DSL parse and semantic validation
- diagnostic aggregation and error spans

### `Charisma.Generator`

- writes generated C# surface from schema IR
- deterministic output routing and headers

### `Charisma.QueryEngine`

- operation model contracts
- planner (`PostgresSqlPlanner`)
- executor (`PostgresSqlExecutor`)
- metadata and exception hierarchy

### `Charisma.Runtime`

- runtime composition API (`CharismaRuntime`)
- provider options and connection handling
- JSON wrapper utilities

### `Charisma.Migration`

- pull introspection
- push schema generation
- migration planning and runner
- destructive safety checks

### `Charisma.Client`

- CLI command parsing and orchestration
- `generate`, `db pull`, `db push`

### `Charisma.All`

- packaging aggregator (`CharismaORM`)

## Key Package Metadata

- CLI package id: `CharismaCLI`
- Tool command name: `charisma`
- Aggregated package id: `CharismaORM`
