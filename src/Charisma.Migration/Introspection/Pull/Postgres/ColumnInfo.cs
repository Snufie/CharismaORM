using System;

namespace Charisma.Migration.Postgres;

internal sealed record ColumnInfo(
    string TableName,
    string ColumnName,
    string PgDataType,
    string UdType,
    bool IsNullable,
    string? DefaultValue,
    bool IsPrimaryKey,
    bool IsUnique,
    int Ordinal,
    int? CharacterMaximumLength);
