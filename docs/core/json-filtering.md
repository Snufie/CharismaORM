# Charisma JSON Filtering

Charisma exposes JSON filtering as first-class query syntax for PostgreSQL `jsonb` columns. This page focuses on filter shapes, examples, and planner translation notes.

## Runtime type and storage

- The runtime provides a `Json` wrapper type (`src/Charisma.Runtime/Json/Json.cs`) that wraps `JsonElement` and normalizes serialization.
- JSON columns must be backed by a provider JSON type (Postgres `jsonb` recommended).

## Common filter shapes

- Path equals: compare a value at a JSON path.
- Array predicates: `Has`, `HasSome`, `HasEvery` for arrays inside JSON.
- Existence/length: check presence or emptiness of a JSON array/object.

Example (path equals):

```csharp
var matches = await client.Article.FindManyAsync(new ArticleFindManyArgs {
 Where = new ArticleWhere { Metadata = new JsonFilter { Path = "$.tags[0]", Equals = "release" } }
});
```

Example (array contains):

```csharp
var hasTag = await client.Article.FindManyAsync(new ArticleFindManyArgs {
 Where = new ArticleWhere { Metadata = new JsonFilter { Path = "$.tags", ArrayContains = "release" } }
});
```

## SQL translation notes

- Filters are translated to provider-specific JSONB operators (Postgres uses `->`, `->>`, `@>`, and `jsonb_path_query` where appropriate).
- All filter values are parameterized; path components are validated and escaped.

## Limitations and performance

- JSON filters are powerful but can be slower than typed scalar columns. Add appropriate GIN/JSONB indexes for frequent JSON queries.
- Not all JSON operations are available in the high-level API; complex needs may require raw SQL or custom executor hooks.

## Tests and examples

- See `tests/Charisma.QueryEngine.Tests/QueryPlannerJsonTests.cs` and `JsonFilterPlannerTests.cs` for planner-level expectations and SQL examples.

## Best practices

- Prefer typed scalar fields for frequently queried attributes.
- Use JSON fields for semi-structured data where flexibility is more important than query speed.
- Index carefully and measure query plans for production workloads.
