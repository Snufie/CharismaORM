# Transactions

Transactions are available both at executor and generated-client levels.

## Contracts

Primary interfaces:

- `ISqlExecutor` (`src/Charisma.QueryEngine/Execution/ISqlExecutor.cs`)
- `ITransactionScope` (`src/Charisma.QueryEngine/Execution/ITransactionScope.cs`)

## Generated Client API

Generated client exposes transaction execution like:

```csharp
await client.TransactionAsync(async tx =>
{
    var created = await tx.Robot.CreateAsync(...);
    var updated = await tx.Robot.UpdateByIdAsync(...);
});
```

Inside transaction callback, model delegates are bound to ambient transaction context.

## Manual Rollback

`ITransactionScope` supports:

- `FailAndRollback(string? reason)`
- `RollbackAsync()`
- `CommitAsync()`

`FailAndRollback` raises a dedicated rollback exception path by design.

## Safety and Lifecycle

- Uncommitted scopes are rolled back on disposal.
- Commit is explicit.
- Context includes the active connection + transaction pair.

## Recommended Use

Use transactions for:

- multi-step write flows
- parent-child write consistency
- conditional update/delete chains

Avoid wrapping long-running external I/O inside DB transactions.

## Related Exceptions

- `CharismaTransactionException`
- `ManualTransactionRollbackException`
