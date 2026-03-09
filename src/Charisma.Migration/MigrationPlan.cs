using System;
using System.Collections.Generic;
using System.Linq;

namespace Charisma.Migration;

/// <summary>
/// Ordered steps required to align the database with the schema, plus diagnostic warnings.
/// </summary>
public sealed record MigrationPlan(
    IReadOnlyList<MigrationStep> Steps,
    IReadOnlyList<string> Warnings,
    IReadOnlyList<string> Unexecutable)
{
    public bool HasDestructiveChanges => Steps.Any(step => step.IsDestructive);
    public bool HasWarnings => Warnings.Count > 0;
    public bool HasUnexecutable => Unexecutable.Count > 0;

    public static readonly MigrationPlan Empty = new(Array.Empty<MigrationStep>(), Array.Empty<string>(), Array.Empty<string>());
}

/// <summary>
/// Single migration action description.
/// </summary>
public sealed record MigrationStep(string Description, bool IsDestructive, string? Sql = null);
