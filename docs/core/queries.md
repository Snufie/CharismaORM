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

Shorthand assignments

The generator now emits implicit conversions for scalar and enum filter types, so you can use a concise assignment instead of constructing a filter object explicitly. Examples:

```csharp
Where = new PostWhere {
  Title = "release",            // shorthand for new StringFilter { Equals = "release" }
  Published = true,              // shorthand for new BoolFilter { Equals = true }
  AuthorId = userId               // shorthand for new GuidFilter { Equals = userId }
}

// For relations, nested where inputs also accept shorthand on scalar fields:
Where = new PostWhere {
  Author = new AuthorRelationFilter {
    Is = new AuthorWhere { Id = userId } // Id can be assigned directly
  }
}
```

## Relation filters

Relation filters let you express conditions over related records. There are two shapes:

- Collection relation filters (one-to-many): `Some`, `None`, `Every`.
- Single relation filters (many-to-one / one-to-one): `Is`, `IsNot`.

When to use each:

- `Some`: true when at least one related record matches the nested filter.
  - Example: posts that have at least one published comment.
  - Use when you want to assert existence of any matching child row.

- `None`: true when zero related records match the nested filter.
  - Example: posts that have no comments containing the word "spam".
  - Use to exclude parent rows that have undesirable children.

- `Every`: true when all related records match the nested filter (vacuously true for empty collections).
  - Example: posts whose every comment is approved.
  - Be careful: `Every` is true for empty child collections — combine with `Some`/`None` if you need different semantics.

- `Is`: for single relations, true when the related single record satisfies the nested filter.
  - Example: an order whose `Customer` satisfies `Country = 'NL'`.
  - Use when filtering by properties of the single linked entity.

- `IsNot`: for single relations, true when the related single record does not satisfy the nested filter (or when the relation is null depending on your model).
  - Example: fetch items where the linked `Owner` is not in a given role.

Examples

Posts with at least one published comment:

```csharp
Where = new PostWhere {
    Comments = new CommentListRelationFilter {
        Some = new CommentWhere { Published = new BoolFilter { Equals = true } }
    }
}
```

Posts where every comment is approved (note: empty-comments posts are vacuously true):

```csharp
Where = new PostWhere {
    Comments = new CommentListRelationFilter {
        Every = new CommentWhere { Approved = new BoolFilter { Equals = true } }
    }
}
```

Filter by a single relation's fields:

```csharp
Where = new OrderWhere {
    Customer = new CustomerRelationFilter {
        Is = new CustomerWhere { Country = new StringFilter { Equals = "NL" } }
    }
}
```

Why not `field == value`?

The generated filter objects are explicit, strongly-typed shapes that map to SQL-planner behavior. Allowing a raw `field == value` expression would require either:

- A lambda-expression DSL (parsing/serializing expression trees), or
- A major change to the generated API to accept arbitrary expression trees or runtime predicates.

Both options are significant design changes (affecting generation, planner, and serialization). The current `Where` / `*Filter` shape keeps the surface explicit and safe, is easy to statically type-check, and maps cleanly to parameterized SQL. If you want a more ergonomic lambda-style API later, it can be added as a higher-level convenience wrapper that translates lambdas into the existing filter objects; it would be a separate feature rather than a small refactor.

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
