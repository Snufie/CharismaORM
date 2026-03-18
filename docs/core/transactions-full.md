# Charisma Transactions: Full Usage

## Transaction Block

```csharp
await client.TransactionAsync(async trx =>
{
    var robot = await trx.Robot.CreateAsync(...);
    if (robot.Name.StartsWith("R"))
        await trx.Robot.UpdateAsync(...);
    // Custom business logic
});
```

## Manual Rollback/Commit

```csharp
await client.TransactionAsync(async trx =>
{
    if (shouldRollback)
        trx.FailAndRollback("Custom reason");
    await trx.CommitAsync();
});
```

## Nested Queries & Business Logic

- Use any C# logic inside transaction block.
- All operations are atomic.
- Rollback on exception or manual call.
