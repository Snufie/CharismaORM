# Advanced Transactions

This page covers advanced patterns: savepoints, explicit failure handling, error translation and integration with connection pooling.

## Savepoints and nested transactions

When you call `TransactionAsync` inside an already open transaction, the runtime will use savepoints where the provider supports them. This allows partial rollbacks without aborting the outer transaction.

```csharp
await client.TransactionAsync(async trx =>
{
    await trx.Robot.CreateAsync(...);

    await trx.TransactionAsync(async inner =>
    {
        // if this fails, only the inner savepoint is rolled back
        inner.FailAndRollback("inner-failure");
    });

    // outer transaction can continue or be rolled back separately
});
```

Provider note: behavior depends on the database provider; test that savepoints are supported and behave as you expect.

## Explicit failure handling and error translation

Use `FailAndRollback` to abort with a business reason. The runtime surfaces the reason in logs and exception messages to aid troubleshooting.

For SQL errors, inspect the inner exception for SQLSTATE and executor details. The runtime attempts to translate common SQL errors to meaningful exceptions.

## Long-running transactions and timeouts

- Avoid long-running work inside transactions. If you must, set appropriate command timeouts on the `ConnectionProvider` and consider background job patterns for slow tasks.

## Connection pooling and scaling

- Register `CharismaClient` with a lifetime that matches your concurrency model. For ASP.NET applications, `Scoped` is a sensible default.
- Ensure your connection pool size accommodates expected concurrent transactions under peak load.

## Testing transactions

- For integration tests, prefer transactional test harnesses that rollback at the end of each test to keep databases clean.
- Use an embedded or ephemeral DB instance for CI to isolate tests from developer environments.

## Example: returning a computed result

```csharp
var stats = await client.TransactionAsync(async trx =>
{
    await trx.Robot.CreateAsync(...);
    var count = await trx.Robot.CountAsync(new RobotCountArgs { Where = new RobotWhere { OwnerId = new StringFilter { Equals = "u1" } } });
    return new { Created = true, TotalForOwner = count };
});
```

If the transaction rolls back, the returned value is discarded and an exception is propagated.
