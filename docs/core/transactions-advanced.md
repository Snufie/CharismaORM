# Charisma Transactions: Advanced Usage

Charisma supports transactional operations with full C# logic inside transaction blocks.

## Example: Transaction Block

```csharp
using Charisma.Runtime;

using var client = new CharismaClient(options);

await client.TransactionAsync(async trx =>
{
    // Regular C# logic
    var robot = await trx.Robot.CreateAsync(...);
    if (robot.Name.StartsWith("R"))
    {
        await trx.Robot.UpdateAsync(...);
    }
    // Custom business logic, loops, etc.
});
```

## Features

- All operations inside the block are atomic.
- Supports async/await, custom logic, and multiple model operations.
- Rollback on exception.

## Tips

- You can nest queries, perform checks, and use any C# code inside the transaction block.
- See also [core/transactions.md](transactions.md) for basic usage.
