using System.Collections.Generic;
using Charisma.QueryEngine.Metadata;

namespace Charisma.QueryEngine.Planning;

/// <summary>
/// Represents a parameterized SQL command emitted by a planner.
/// </summary>
public sealed record SqlPlan(
    SqlPlanKind Kind,
    string CommandText,
    IReadOnlyList<SqlParameterValue> Parameters,
    ModelMetadata? Model = null,
    IncludePlan? IncludeRoot = null,
    IReadOnlyList<string>? DistinctFields = null,
    int? PostDistinctSkip = null,
    int? PostDistinctTake = null);

/// <summary>
/// Indicates the expected shape of the plan output.
/// </summary>
public enum SqlPlanKind
{
    QuerySingle,
    QueryMany,
    NonQuery
}

/// <summary>
/// Provider-agnostic parameter value.
/// </summary>
public sealed record SqlParameterValue(string Name, object? Value);

/// <summary>
/// Description of a nested include/join tree for materialization.
/// </summary>
public sealed record IncludePlan(
    string RelationName,
    ModelMetadata Meta,
    string Alias,
    string ParentAlias,
    string ColumnPrefix,
    bool IsCollection,
    IReadOnlyList<string> ParentColumns,
    IReadOnlyList<string> ChildColumns,
    IReadOnlyList<IncludePlan> Children,
    IReadOnlyList<string> Columns);
