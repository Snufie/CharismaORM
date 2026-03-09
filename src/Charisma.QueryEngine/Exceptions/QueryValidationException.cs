using System;
using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Exceptions;

/// <summary>
/// Thrown when a generated delegate or caller provides an invalid or unsupported query payload.
/// </summary>
public sealed class QueryValidationException : Exception
{
    public string ModelName { get; }
    public QueryType Operation { get; }

    /// <summary>
    /// Creates a validation exception for an invalid delegate/query payload.
    /// </summary>
    /// <param name="modelName">Model targeted by the invalid query.</param>
    /// <param name="operation">Operation attempted.</param>
    /// <param name="message">Validation error message.</param>
    public QueryValidationException(string modelName, QueryType operation, string message)
        : base(message)
    {
        ModelName = modelName;
        Operation = operation;
    }
}
