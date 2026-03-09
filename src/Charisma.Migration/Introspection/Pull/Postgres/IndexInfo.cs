namespace Charisma.Migration.Postgres;

internal sealed record IndexInfo(
    string TableName,
    string IndexName,
    bool IsUnique,
    bool IsPrimary,
    IReadOnlyList<string> Columns);