using System;

namespace Charisma.QueryEngine.Exceptions;

/// <summary>
/// Base exception for query execution failures.
/// </summary>
public class CharismaQueryException : Exception
{
    public string ModelName { get; }
    public string Operation { get; }

    /// <summary>
    /// Creates a base query exception with model/operation context.
    /// </summary>
    /// <param name="modelName">Model involved in the failing query.</param>
    /// <param name="operation">Operation name (e.g., findUnique, updateMany).</param>
    /// <param name="message">Human-readable message.</param>
    /// <param name="inner">Optional underlying exception.</param>
    public CharismaQueryException(string modelName, string operation, string message, Exception? inner = null)
        : base(message, inner)
    {
        ModelName = modelName;
        Operation = operation;
    }
}

/// <summary>
/// Thrown when a unique constraint is violated (Postgres SQLSTATE 23505).
/// </summary>
public sealed class UniqueConstraintViolationException : CharismaQueryException
{
    public string? ConstraintName { get; }

    /// <summary>
    /// Creates a unique constraint violation exception.
    /// </summary>
    /// <param name="modelName">Model involved.</param>
    /// <param name="operation">Operation that triggered the constraint.</param>
    /// <param name="constraintName">Database constraint name, if known.</param>
    /// <param name="inner">Optional underlying exception.</param>
    public UniqueConstraintViolationException(string modelName, string operation, string? constraintName, Exception? inner = null)
        : base(modelName, operation, BuildMessage(modelName, constraintName), inner)
    {
        ConstraintName = constraintName;
    }

    /// <summary>
    /// Builds an exception message from model and constraint context.
    /// </summary>
    private static string BuildMessage(string modelName, string? constraintName)
    {
        return constraintName is null
            ? $"Unique constraint violation while executing '{modelName}'."
            : $"Unique constraint '{constraintName}' violated on '{modelName}'.";
    }
}

/// <summary>
/// Thrown when a foreign key constraint fails (Postgres SQLSTATE 23503).
/// </summary>
public sealed class ForeignKeyViolationException : CharismaQueryException
{
    public string? ConstraintName { get; }

    /// <summary>
    /// Creates a foreign key violation exception.
    /// </summary>
    /// <param name="modelName">Model involved.</param>
    /// <param name="operation">Operation that triggered the constraint.</param>
    /// <param name="constraintName">Database constraint name, if known.</param>
    /// <param name="inner">Optional underlying exception.</param>
    public ForeignKeyViolationException(string modelName, string operation, string? constraintName, Exception? inner = null)
        : base(modelName, operation, BuildMessage(modelName, constraintName), inner)
    {
        ConstraintName = constraintName;
    }

    /// <summary>
    /// Builds an exception message from model and constraint context.
    /// </summary>
    private static string BuildMessage(string modelName, string? constraintName)
    {
        return constraintName is null
            ? $"Foreign key constraint violated while executing '{modelName}'."
            : $"Foreign key constraint '{constraintName}' violated on '{modelName}'.";
    }
}

/// <summary>
/// Thrown when an operation that should affect a row does not find a matching record.
/// </summary>
public sealed class RecordNotFoundException : CharismaQueryException
{
    /// <summary>
    /// Creates a record-not-found exception for a given model/operation.
    /// </summary>
    public RecordNotFoundException(string modelName, string operation, Exception? inner = null)
        : base(modelName, operation, $"No matching {modelName} found for operation '{operation}'.", inner)
    {
    }
}

/// <summary>
/// Thrown when an operation results in a zero-row touch where at least one row was expected.
/// A.K.A. This operation touched the pitch-black void and nothing answered.
/// </summary>
public sealed class VoidTouchException : CharismaQueryException
{
    /// <summary>
    /// Creates a zero-rows-touched exception for a given model/operation.
    /// </summary>
    public VoidTouchException(string modelName, string operation, Exception? inner = null)
        : base(modelName, operation, $"Operation '{operation}' on model '{modelName}' touched zero rows.", inner)
    {
    }
}
