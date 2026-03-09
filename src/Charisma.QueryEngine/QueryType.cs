namespace Charisma.QueryEngine.Model;

/// <summary>
/// Enumerates all supported query operations.
/// This enum is consumed by generated delegates and the SQL builder.
/// </summary>
public enum QueryType
{
    FindUnique,
    FindFirst,
    FindMany,
    Create,
    CreateMany,
    Update,
    UpdateMany,
    Upsert,
    Delete,
    DeleteMany,
    Count,
    Aggregate,
    GroupBy
}
