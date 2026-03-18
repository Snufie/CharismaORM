# Sugar Methods

Charisma emits convenience ("sugar") methods on each model delegate to make common tasks ergonomically simple. These are generated in addition to the full `*Args`-style APIs.

Common generated sugar methods

- `FindByIdAsync(id)` or `FindBy{Key}Async(...)` — find by primary or unique key.
- `FindManyAsync(...)` — light-weight overloads accepting lambdas or small arg shorthand.
- `CountAsync(...)` — returns matching row count.
- `ExistsAsync(...)` — boolean presence check.

Why use sugar methods

- Shorter call sites for common operations.
- Lambda overloads are convenient in small queries and in LINQ-style code.
- Generated sugar methods map to the same planner and are efficient.

Examples

Find by single primary key (generated helper):

```csharp
var robot = await client.Robot.FindByIdAsync(Guid.Parse("...") );
```

Find by unique key (generated `FindBy*`):

```csharp
var user = await client.User.FindByEmailAsync("alice@example.com");
```

FindMany — lambda overload vs `*Args` object

Lambda-style:

```csharp
var active = await client.Robot.FindManyAsync(
 where: r => r.Status == RobotStatus.Active,
 orderBy: r => r.CreatedAt.Desc(),
 take: 10,
 select: r => new { r.Id, r.Name }
);
```

Args-style (explicit generated types):

```csharp
var results = await client.Robot.FindManyAsync(new RobotFindManyArgs {
 Where = new RobotWhere { Status = new EnumFilter<RobotStatus> { Equals = RobotStatus.Active } },
 OrderBy = new[] { new RobotOrderBy { CreatedAt = OrderByDirection.Desc } },
 Take = 10,
 Select = new RobotSelect { Id = true, Name = true }
});
```

Exists and Count examples

```csharp
bool exists = await client.Robot.ExistsAsync(r => r.Name == "R-001");
int count = await client.Robot.CountAsync(r => r.OwnerId == ownerId);
```

Composite key helpers

If your model uses a composite key (`@@id([a,b])`) the generator emits `FindByCompositeAsync` or a `FindBy{FieldA}And{FieldB}Async` helper matching the key names. Prefer these helpers for concise, intention-revealing code.

Projection and includes

- Most sugar helpers accept a `select` or `include` lambda to shape returned data. When you need fine-grained control, use the `*Args` types.
- Example include:

```csharp
var withTasks = await client.Robot.FindManyAsync(include: r => r.Tasks);
```

Guidance and best practices

- Use sugar helpers for simple reads and presence checks.
- For complex filters (deep relation filtering, JSON filters, or advanced aggregates), use the explicit `*Args` types - they map more directly to planner errors and are easier to debug.
- Use `ExistsAsync` instead of `FindMany` when you only need to test presence.

See also

- [core/delegate-methods.md](delegate-methods.md)
- [core/queries.md](queries.md)
