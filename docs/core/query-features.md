# Charisma Query Features

## Filtering

- Lambda expressions: `FindManyAsync(r => r.Name == "R-001")`
- Filter objects: `FindManyAsync(new RobotFilter { Name = "R-001" })`

## Projection

- Select: `FindManyAsync(..., select: r => new { r.Name })`
- Include: `FindManyAsync(..., include: r => r.Related)`
- Omit: `FindManyAsync(..., omit: r => r.SecretField)`

## Composite Key Support

- Use `@@id([field1, field2])` in schema for composite keys.
- Delegates generate `FindByCompositeAsync` methods.

## GroupBy & Aggregate

- `GroupByAsync(GroupByArgs)`
- `AggregateAsync(AggregateArgs)`

## JSON Filtering

- Filter on JSON fields: `FindManyAsync(r => r.Metadata["type"] == "explorer")`

## Optional Lists

- Nullable collections: `String[]?`, `Int[]?`

## Advanced Patterns

- Combine filters, projections, and includes for rich queries.

## Examples

Below are concrete examples that show how generated query features map to the runtime APIs.

### Filter object vs lambda example

Args-style (explicit):

```csharp
var args = new RobotFindManyArgs {
 Where = new RobotWhere { Name = new StringFilter { Equals = "R-001" }, CreatedAt = new DateTimeFilter { Gte = DateTime.UtcNow.AddDays(-7) } },
 Take = 50
};
var results = await client.Robot.FindManyAsync(args);
```

Lambda-style (convenience):

```csharp
var results = await client.Robot.FindManyAsync(where: r => r.Name == "R-001" && r.CreatedAt >= DateTime.UtcNow.AddDays(-7), take: 50);
```

### Projection and include

```csharp
var list = await client.Post.FindManyAsync(
 where: p => p.Published,
 select: p => new { p.Id, p.Title }
);

var withAuthor = await client.Post.FindManyAsync(include: p => p.Author);
```

### Composite key lookup

If your model defines `@@id([a,b])` the generator emits a composite helper:

```csharp
var entity = await client.MyModel.FindByCompositeAsync((aValue, bValue));
```

### GroupBy & Aggregates

```csharp
var stats = await client.Post.GroupByAsync(new PostGroupByArgs {
 By = new[] { "AuthorId" },
 Aggregates = new PostAggregates { Count = true, Avg = new[] { "ReadCount" } }
});
```

### JSON filtering example

```csharp
var matches = await client.Article.FindManyAsync(new ArticleFindManyArgs {
 Where = new ArticleWhere { Metadata = new JsonFilter { Path = "$.tags", ArrayContains = "release" } }
});
```

### Optional lists — null vs empty

```csharp
var r = await client.Robot.FindByIdAsync(id);
if (r.Tags == null) { /* tags missing */ }
if (r.Tags != null && r.Tags.Length == 0) { /* explicit empty */ }
```

### Tips

- Prefer `ExistsAsync` when only checking presence.
- Use `Select` to limit data returned for wide models.
- For queries involving JSON or complex relation filters prefer the explicit `*Args` shape to improve debuggability.
