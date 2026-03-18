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

## Common argument shapes

- Where: express field-level conditions and logical composition.
- OrderBy: one or more fields with direction and nulls ordering.
- Cursor: a stable position for cursor-based pagination (usually the unique key value(s)).
- Skip / Take: offset/limit style pagination.
- Distinct: fields to deduplicate on.
- Projection: `Select`, `Include`, or `Omit` (mutually exclusive).

Below are examples and patterns you can copy into generated code.

## Basic `FindMany` examples

Object-args style (typical generated shapes):

```csharp
var args = new PostFindManyArgs {
 Where = new PostWhere {
  Title = new StringFilter { Contains = "release", Mode = StringFilterMode.Insensitive },
  AND = new[] {
   new PostWhere { Published = new BoolFilter { Equals = true } },
   new PostWhere { AuthorId = new StringFilter { In = new[] { "a1", "b2" } } }
  }
 },
 OrderBy = new[] { new PostOrderBy { CreatedAt = OrderByDirection.Desc } },
 Skip = 0,
 Take = 20,
 Select = new PostSelect { Id = true, Title = true, CreatedAt = true }
};

var results = await client.Post.FindManyAsync(args);
```

Lambda-style helper (when available):

```csharp
var recent = await client.Post.FindManyAsync(
 where: p => p.Published && p.Title.Contains("release", StringComparison.OrdinalIgnoreCase),
 orderBy: p => p.CreatedAt.Desc(),
 take: 20,
 select: p => new { p.Id, p.Title, p.CreatedAt }
);
```

## `Where` filters — patterns and operators

Scalar fields expose typed filters with operator properties. Common filters:

- equals / notEquals
- `In` / `NotIn` (collections)
- `Lt`, `Lte`, `Gt`, `Gte` (range)
- `Contains`, `StartsWith`, `EndsWith` (strings)
- `Mode` (case-sensitive / insensitive for strings)

Logical composition uses `AND`, `OR`, `NOT` as arrays or single filter values:

```csharp
Where = new PostWhere {
 OR = new[] {
  new PostWhere { Title = new StringFilter { Contains = "alpha" } },
  new PostWhere { Title = new StringFilter { Contains = "beta" } }
 },
 NOT = new[] { new PostWhere { Deleted = new BoolFilter { Equals = true } } }
}
```

## Relation filters

To filter by related collections or single relations use relation-specific filters:

- For one-to-many: `Some`, `None`, `Every` (e.g., comments on posts).
- For single relation: `Is`, `IsNot` with a nested filter.

Example: posts with at least one published comment:

```csharp
Where = new PostWhere {
 Comments = new CommentListRelationFilter { Some = new CommentWhere { Published = new BoolFilter { Equals = true } } }
}
```

## JSON filters

JSON columns support path-based filtering. Typical shape:

```csharp
Where = new ArticleWhere {
 Metadata = new JsonFilter { Path = "$.tags", ArrayContains = "release" }
}
```

Use JSON filters sparingly; prefer typed fields when you need frequent filtering.

## Projection: `Select`, `Include`, `Omit`

- `Select` explicitly lists scalar fields to return.
- `Include` brings related records inline (nested selects inside include are supported where the generator exposes them).
- `Omit` excludes fields from the returned record.

Only one of `Select`, `Include`, or `Omit` may be present on a single query. Examples:

```csharp
// Select only id/title
Select = new PostSelect { Id = true, Title = true }

// Include author and comments
Include = new PostInclude { Author = true, Comments = true }
```

## Cursor and stable pagination

Cursor pagination uses a cursor object representing unique key(s) for a stable tie-breaker. Example:

```csharp
var page = await client.Post.FindManyAsync(new PostFindManyArgs {
 Cursor = new PostWhereUnique { Id = "..." },
 Take = 10
});
```

When `OrderBy` is provided, the planner ensures a stable deterministic order using unique keys as tie-breakers.

## Distinct

`Distinct` accepts one or more scalar fields to deduplicate on. The planner performs checks to ensure distinct semantics are valid with the projection.

## Aggregation and `GroupBy`

Aggregation queries accept aggregate selectors (count, sum, avg, min, max) and `By` keys for grouping. Example:

```csharp
var stats = await client.Post.GroupByAsync(new PostGroupByArgs {
 By = new[] { "AuthorId" },
 Aggregates = new PostAggregates { Count = true, Avg = new[] { "ReadCount" } }
});
```

## `FindUnique` / `FindFirst` behavior

- `FindUnique` expects a unique key (or composite unique keys via `@@id`). It returns at most one record.
- `FindFirst` returns the first record that matches ordering and filters; use `FindFirstOrThrow` when presence is required.

## Mutation query projections and constraints

- `Include` is allowed when the mutation path returns a single record (e.g., `Update`/`Create`).
- Bulk mutation operations (`CreateMany`, `UpdateMany`, `DeleteMany`) return counts and do not support `Include`.

## Security and parameterization

All values are parameterized by the planner to avoid SQL injection. Identifiers are escaped and JSON paths are treated as values when supplied by user code.

## Examples & Tests

Review the following tests for concrete SQL/planner expectations:

- `tests/Charisma.QueryEngine.Tests/PlannerBehaviorTests.cs`
- `tests/Charisma.QueryEngine.Tests/QueryPlannerJsonTests.cs`
- `tests/Charisma.QueryEngine.Tests/QueryPlannerRelationFilterTests.cs`
