using System;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Options controlling how migrations are planned/applied for Postgres.
/// </summary>
public sealed class PostgresMigrationOptions
{
    /// <summary>
    /// Allow potentially destructive steps (drops, type narrowing, making columns not null when data may be present).
    /// </summary>
    public bool AllowDestructive { get; }

    /// <summary>
    /// Allow destructive steps even when data exists (skips emptiness safety checks for drops).
    /// </summary>
    public bool AllowDataLoss { get; }

    /// <summary>
    /// Allow rename operations when the planner detects a likely rename.
    /// </summary>
    public bool AllowRenames { get; }

    public PostgresMigrationOptions(bool allowDestructive = false, bool allowDataLoss = false, bool allowRenames = true)
    {
        AllowDestructive = allowDestructive;
        AllowDataLoss = allowDataLoss;
        AllowRenames = allowRenames;
    }
}
