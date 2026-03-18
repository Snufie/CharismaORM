# Querying Data

CharismaORM query behavior is driven by query models, planner logic, and executor materialization.

## Query Model Layer

`QueryType` and query records are defined in:

- `src/Charisma.QueryEngine/QueryType.cs`
- `src/Charisma.QueryEngine/QueryModel.cs`

Supported operations:

- `FindUnique`
- `FindFirst`
- `FindMany`
- `Create`
- `CreateMany`
- `Update`
- `UpdateMany`
- `Upsert`
- `Delete`
- `DeleteMany`
- `Count`
- `Aggregate`
- `GroupBy`

## Read Query Building

Typical args classes include:

- `Where`
- `OrderBy`
- `Cursor`
- `Skip`
- `Take`
- `Distinct`
- `Select` / `Include` / `Omit`

## Projection Rules

Only one projection mode can be used at a time:

- `Select`
- `Include`
- `Omit`

Providing multiple projection modes results in validation/planning exceptions.

## Filters

Implemented filter patterns include:

- logical composition: `AND`, `OR`, `NOT`, `XOR`
- scalar operators: equals/in/not-in/range operators
- string operators: contains/startsWith/endsWith and mode sensitivity
- relation filters: `Some`, `None`, `Every`, `Is`, `IsNot`
- JSON filters: path and array-specific conditions

## Pagination and Ordering

Supported behavior includes:

- `Skip` + `Take`
- `OrderBy`
- cursor-style pagination
- distinct with planner checks
- stable tie-breaker handling with keys

## Aggregation and Grouping

Aggregate and groupBy are first-class query types with planner support for selection constraints.

Typical selectors include count and numeric aggregations, with group keys from scalar fields.

## Mutation Behavior

Mutation methods route through executor paths that may return:

- single record
- record list
- affected row count

Batch operation variants are separated to keep return shape explicit.

Current executor constraints to keep in mind:

- `Include` is supported for read paths and select single-record mutation paths.
- `Include` is rejected for delete and bulk mutation executor paths (`CreateMany`, `UpdateMany`, `Delete`, `DeleteMany`).
- Nested relation directives are validated per relation and directive; unsupported combinations throw `NotSupportedException`.

## Security Model in Query Planning

Planner behavior is hardened for:

- parameterized values
- escaped/quoted identifiers
- protected JSON path and filter value handling

## Test Anchors

See these for concrete SQL behavior examples:

- `tests/Charisma.QueryEngine.Tests/PlannerBehaviorTests.cs`
- `tests/Charisma.QueryEngine.Tests/QueryPlannerSecurityTests.cs`
- `tests/Charisma.QueryEngine.Tests/QueryPlannerJsonTests.cs`
- `tests/Charisma.QueryEngine.Tests/QueryPlannerPaginationDistinctTests.cs`
- `tests/Charisma.QueryEngine.Tests/QueryPlannerRelationFilterTests.cs`
