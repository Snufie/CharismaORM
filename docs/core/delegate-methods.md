# Charisma Delegate Methods

The generator emits a strongly-typed delegate for each model (for example `client.Post` or `client.Robot`). Delegates provide commonly-used async methods for CRUD, queries, aggregates and convenience accessors. Below are the typical method shapes and examples you can expect from generated code.

## Typical method patterns

- FindUnique / FindUniqueAsync
- FindFirst / FindFirstAsync / FindFirstOrThrowAsync
- FindMany / FindManyAsync
- Create / CreateAsync
- CreateMany / CreateManyAsync
- Update / UpdateAsync
- UpdateMany / UpdateManyAsync
- Delete / DeleteAsync
- DeleteMany / DeleteManyAsync
- Count / CountAsync
- Exists / ExistsAsync
- GroupBy / GroupByAsync

All async methods follow `Task<T>` or `Task` return shapes. The generator produces `*Args` types for object-style calls and convenience lambda overloads for inline expressions where possible.

## Example signatures (patterns)

- `Task<T?> FindUniqueAsync(TUniqueArgs args)`
- `Task<T?> FindFirstAsync(TFindFirstArgs args)`
- `Task<IEnumerable<T>> FindManyAsync(TFindManyArgs args)`
- `Task<T> CreateAsync(TCreateArgs args)`
- `Task<int> CreateManyAsync(TCreateManyArgs args)`
- `Task<T> UpdateAsync(TUpdateArgs args)`
- `Task<int> UpdateManyAsync(TUpdateManyArgs args)`
- `Task<T> DeleteAsync(TDeleteArgs args)`
- `Task<int> DeleteManyAsync(TDeleteManyArgs args)`
- `Task<int> CountAsync(TCountArgs args)`
- `Task<bool> ExistsAsync(TExistsArgs args)`
- `Task<IEnumerable<GroupResult>> GroupByAsync(TGroupByArgs args)`

These signatures are patterns rather than literal type names — check the generated client headers for exact type names like `PostFindManyArgs`, `RobotWhere`, etc.

## Usage examples

Basic find:

```csharp
var post = await client.Post.FindUniqueAsync(new PostFindUniqueArgs { Where = new PostWhereUnique { Id = id } });
```

Find many with filters and projection:

```csharp
var recent = await client.Post.FindManyAsync(new PostFindManyArgs {
 Where = new PostWhere { Published = new BoolFilter { Equals = true } },
 OrderBy = new[] { new PostOrderBy { CreatedAt = OrderByDirection.Desc } },
 Take = 10,
 Select = new PostSelect { Id = true, Title = true }
});
```

Exists / Count convenience:

```csharp
bool hasActive = await client.Robot.ExistsAsync(new RobotExistsArgs { Where = new RobotWhere { Status = new EnumFilter<RobotStatus> { Equals = RobotStatus.Active } } });
int total = await client.Robot.CountAsync(new RobotCountArgs { Where = new RobotWhere { OwnerId = new StringFilter { Equals = ownerId } } });
```

GroupBy example:

```csharp
var stats = await client.Robot.GroupByAsync(new RobotGroupByArgs {
 By = new[] { "Status" },
 Aggregates = new RobotAggregates { Count = true }
});
```

## Best practices

- Prefer projection (`Select`) to reduce data transfer for wide models.
- Use `FindUnique` when you have a unique key; it provides clearer intent and helps the planner.
- Use `Exists` for presence checks instead of performing a full `Find`.
- For complex filters, construct the `*Where` object form rather than large inline lambda expressions — it maps more directly to planner tests and errors.

## See also

- [Querying Data](queries.md)
- [Query Features](query-features.md)
- [Sugar Methods](sugar-methods.md)
- [Transactions](transactions-full.md)
