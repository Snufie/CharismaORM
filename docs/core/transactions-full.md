# Charisma Transactions — Usage

This page documents the recommended patterns for using transactions with `CharismaClient`.

## Transaction scope (recommended)

Use `TransactionAsync` to run a block of operations atomically. The transaction context (`trx`) exposes the same model delegates as the root client and can be used with `await` inside ordinary C# logic.

```csharp
await client.TransactionAsync(async trx =>
{
    var robot = await trx.Robot.CreateAsync(new RobotCreateArgs { Data = new RobotCreateInput { Name = "R-1" } });
    if (robot.Name.StartsWith("R"))
    {
        await trx.Robot.UpdateAsync(new RobotUpdateArgs { Where = new RobotWhereUnique { Id = robot.Id }, Data = new RobotUpdateInput { Status = RobotStatus.Active } });
    }
});
```

Behavior notes

- The transaction is committed automatically when the delegate completes successfully.
- An unhandled exception will cause an automatic rollback.
- All operations inside the block share the same database connection and transaction context.

## Manual rollback and explicit commit

You can request an explicit rollback or commit from the transaction context if you need programmatic control.

```csharp
await client.TransactionAsync(async trx =>
{
    if (shouldRollback)
    {
        trx.FailAndRollback("business-rule-failed");
        return; // transaction is rolled back
    }

    await trx.CommitAsync(); // optional: commit early
});
```

Use `FailAndRollback` to abort and rollback with a clear reason; commit early only when you intentionally want to finalize while still inside the block.

## Returning values from transactions

Transaction lambdas can return values. The runtime returns the value only if the transaction commits.

```csharp
var result = await client.TransactionAsync(async trx =>
{
    var r = await trx.Robot.CreateAsync(new RobotCreateArgs { Data = new RobotCreateInput { Name = "X" } });
    return r.Id;
});
```

If the transaction rolls back, an exception is thrown and no value is returned.

## Nested transaction semantics

Charisma supports nested transactional scopes via savepoints when the provider supports them. Nested `TransactionAsync` calls within an outer transaction typically create a savepoint; failing the inner scope rolls back to that savepoint without aborting the outer transaction (provider-dependent).

Guidance:

- Prefer a single transaction scope for a related unit of work.
- Use nested scopes only when you need independent rollback boundaries; test provider behavior for savepoint semantics.

## Performance considerations

- Keep transactions short to reduce lock contention.
- Avoid long-running external calls while holding a transaction.
- Use connection pooling for high-concurrency workloads.

## Troubleshooting

- If you see unexpected rollbacks, inspect inner exceptions for planner/executor SQL errors.
- Ensure the transaction scope lifetime is tied to the operation scope and not captured by long-lived closures.
