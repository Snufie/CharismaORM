# Error Reference

This page lists major exception categories across parser, query engine, and migrations.

## Parser Errors

### `CharismaSchemaException`

Represents a single parser/validator diagnostic, optionally with source span.

### `CharismaSchemaAggregateException`

Represents multiple parser diagnostics collected in one parse pass.

Typical causes:

- invalid block syntax
- duplicate symbols
- invalid relation/default usage

## Query Engine Errors

### `QueryValidationException`

Raised when args/query payload violate validation rules before execution.

### `CharismaQueryException` (base)

Base type for query operation failures.

Derived types include:

- `UniqueConstraintViolationException`
- `ForeignKeyViolationException`
- `RecordNotFoundException`
- `VoidTouchException`
- `DatabaseExecutionException`

## Transaction Errors

### `CharismaTransactionException` (base)

Base transaction exception category.

### `ManualTransactionRollbackException`

Raised by explicit rollback shortcut (`FailAndRollback`).

## Migration Errors

Migration apply/planning errors generally surface as operation exceptions with clear message channels:

- unexecutable plan blocks
- warning/data-loss gate failures
- destructive gate failures

## Debug Checklist

When handling errors, inspect in this order:

1. command and args used
2. schema validity and generation state
3. runtime option configuration
4. database connectivity and state
5. exception type and SQLSTATE details (where available)
