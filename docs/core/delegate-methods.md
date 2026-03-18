# Charisma Delegate Methods

Charisma generates a rich set of methods for each model delegate. Below is a complete list with signatures and usage examples.

## CRUD & Query Methods

## FindById & FindBy\* Methods

## Convenience Methods

## Usage Example

```csharp
var robot = await client.Robot.FindByIdAsync(Guid.Parse("..."));
var exists = await client.Robot.ExistsAsync(r => r.Name == "R-001");
var count = await client.Robot.CountAsync(r => r.Status == RobotStatus.Active);
```

## Method Signatures

All methods are async and return `Task` or `Task<T>`. See generated code headers for full signatures and overloads.

## Troubleshooting & Best Practices

- Always check for null when using `FindFirst`/`FindUnique`.
- Use `FindFirstOrThrow` for strict existence checks.
- Prefer filter lambdas for simple queries, filter objects for complex conditions.
- Use projections to minimize data transfer.
- For composite keys, use `FindByCompositeAsync` or custom `FindBy*` methods.

## Advanced Usage Examples

### Find by composite key

```csharp
var robot = await client.Robot.FindByCompositeAsync((serial, batch));
```

### Filtering and projection

```csharp
var robots = await client.Robot.FindManyAsync(r => r.Status == RobotStatus.Active, select: r => new { r.Name, r.CreatedAt });
```

### Including related data

```csharp
var robots = await client.Robot.FindManyAsync(include: r => r.Tasks);
```

### Exists and Count

```csharp
bool exists = await client.Robot.ExistsAsync(r => r.Name == "R-001");
int count = await client.Robot.CountAsync(r => r.Status == RobotStatus.Active);
```

### Aggregate and GroupBy

```csharp
var stats = await client.Robot.GroupByAsync(new GroupByArgs { By = new[] { "Status" }, Aggregates = ... });
```

## See Also

- [Query Features](query-features.md)
- [Sugar Methods](sugar-methods.md)
- [Transactions](transactions-full.md)
