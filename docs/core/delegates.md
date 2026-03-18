# Charisma Delegates: Usage and Examples

Charisma generates delegates for each model, providing strongly-typed CRUD and query methods.

## Example: CRUD Operations

```csharp
using MyApp.Generated;
using Charisma.Runtime;

var client = new CharismaClient(options);

// Create
var created = await client.Robot.CreateAsync(new RobotCreateArgs { Data = new RobotCreateInput { Name = "R-001" } });

// Read
var robot = await client.Robot.FindByIdAsync(Guid.Parse("..."));
var all = await client.Robot.FindManyAsync();

// Update
await client.Robot.UpdateAsync(new RobotUpdateArgs { Where = ..., Data = ... });

// Delete
await client.Robot.DeleteAsync(new RobotDeleteArgs { Where = ... });
```

## Sugar Methods

- `FindBy*`: Find by unique/composite keys (e.g., `FindBySerialAsync`)
- `FindManyAsync`: Query with filters, pagination, projection
- `CountAsync`: Count matching records

## Composite IDs and @@ Blocks

- Composite keys are supported via `@@id([field1, field2])` in schema.
- Delegates generate `FindByCompositeAsync` methods for composite keys.

## Special Features

- `@@unique`, `@@index`, `@@map` blocks are mapped to query and filter methods.
- Delegates expose advanced query/filtering for these features.

## Full Type and Method Coverage

- All generated delegates are documented in the generated code headers.
- See also [core/queries.md](queries.md) for advanced query usage.
