using System.Collections.Generic;

namespace Charisma.Migration.Introspection.Push.Postgres;

internal sealed class DbSnapshot
{
    public IReadOnlyDictionary<string, DbEnum> Enums { get; }
    public IReadOnlyDictionary<string, DbTable> Tables { get; }

    public DbSnapshot(
        IReadOnlyDictionary<string, DbEnum> enums,
        IReadOnlyDictionary<string, DbTable> tables)
    {
        Enums = enums;
        Tables = tables;
    }
}

internal sealed record DbEnum(string Name, IReadOnlyList<string> Values);

internal sealed record DbTable(
    string Name,
    IReadOnlyDictionary<string, DbColumn> Columns,
    IReadOnlyList<string> PrimaryKey,
    IReadOnlyList<DbUnique> Uniques,
    IReadOnlyList<DbIndex> Indexes,
    IReadOnlyList<DbForeignKey> ForeignKeys);

internal sealed record DbColumn(
    string Name,
    string DataType,
    bool IsNullable,
    string? DefaultValue,
    int? CharacterMaximumLength,
    bool IsPrimaryKey,
    bool IsUnique,
    bool IsEnum);

internal sealed record DbUnique(string Name, IReadOnlyList<string> Columns);

internal sealed record DbIndex(string Name, IReadOnlyList<string> Columns, bool IsUnique, bool IsPrimary);

internal sealed record DbForeignKey(
    string Name,
    IReadOnlyList<string> LocalColumns,
    string ForeignTable,
    IReadOnlyList<string> ForeignColumns,
    string DeleteRule,
    string UpdateRule);
