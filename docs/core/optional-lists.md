# Optional Lists

Optional lists are nullable collection fields (e.g., `String[]?`) and are useful when a distinction between "missing" and "empty" matters.

Schema example

```charisma
model Robot {
  id   Id @id @default(uuid())
  tags String[]? // Optional list of tags
}
```

C# usage

```csharp
var robot = await client.Robot.FindByIdAsync(new RobotFindUniqueArgs { Where = new RobotWhereUnique { Id = id } });
if (robot.Tags == null)
{
  // tags are missing (null)
}
else if (robot.Tags.Length == 0)
{
  // tags are present but empty
}
else
{
  foreach (var t in robot.Tags) { /* ... */ }
}
```

Query patterns

- Check for null: `Where = new RobotWhere { Tags = new JsonFilter { IsNull = true } }` or use lambda helpers when available.
- Check for empty vs non-empty: test `Length` if generator exposes it, or use array containment filters (e.g., `Has`/`HasSome`).

Database and generator notes

- Null vs empty semantics map to the underlying column nullability. On Postgres, a nullable `text[]` can be `NULL` or `ARRAY[]`.
- The generator emits C# arrays and nullable annotations to reflect schema nullability.

Best practices

- Prefer `Type[]` (non-nullable) when the empty list is a valid default and you do not need to distinguish missing data.
- Use optional lists when a missing value has business meaning distinct from an empty collection.
- Document the chosen semantics for critical models to avoid surprises for API consumers.
