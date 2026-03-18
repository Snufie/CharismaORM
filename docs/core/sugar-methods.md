# Charisma Sugar Methods

Charisma generates convenient sugar methods for common queries.

## Examples

- `FindByIdAsync`: Find by primary key
- `FindBy*Async`: Find by unique/composite keys
- `CountAsync`: Count records
- `ExistsAsync`: Check existence

## Usage

```csharp
var robot = await client.Robot.FindBySerialAsync("SN-001");
var exists = await client.Robot.ExistsAsync(r => r.Name == "R-001");
var count = await client.Robot.CountAsync(r => r.CreatedAt > DateTime.UtcNow.AddDays(-7));
```

## Custom Filters

- Use lambda expressions or filter objects for advanced queries.
- Combine sugar methods with projections and includes.

## See Also

- [core/delegates.md](delegates.md)
- [core/queries.md](queries.md)
