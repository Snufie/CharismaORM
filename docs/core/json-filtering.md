# Charisma JSON Filtering

Charisma supports advanced JSON filtering in queries, allowing you to filter on nested JSON fields directly in the database.

## Example: Filter by JSON Field

```csharp
var robots = await client.Robot.FindManyAsync(r => r.Metadata["type"] == "explorer");
```

## How It Works

- JSON fields are mapped to C# dictionaries or objects.
- Filters are translated to SQL JSON operators (PostgreSQL).
- Supports equality, containment, and path queries.

## Limitations

- Only works with PostgreSQL JSON/JSONB columns.
- Not all operators are supported; see generated code for details.
- Complex queries may require manual SQL or custom logic.

## Why Unique?

- Charisma exposes JSON filtering as first-class query syntax, unlike most ORMs.
- Enables rich filtering and querying of semi-structured data.
