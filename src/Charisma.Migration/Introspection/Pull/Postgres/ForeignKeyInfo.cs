namespace Charisma.Migration.Postgres;

internal sealed record ForeignKeyInfo(
    string TableName,
    string ColumnName,
    string ReferencedTable,
    string ReferencedColumn,
    string ConstraintName,
    bool IsNullable,
    string DeleteRule);
