using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Charisma.Migration.Introspection.Push.Postgres;

internal sealed class PostgresDatabaseIntrospector
{
    private readonly string _connectionString;

    public PostgresDatabaseIntrospector(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public async Task<DbSnapshot> ReadAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var enums = await ReadEnumsAsync(conn, cancellationToken).ConfigureAwait(false);
        var tables = await ReadTablesAsync(conn, enums, cancellationToken).ConfigureAwait(false);

        return new DbSnapshot(enums, tables);
    }

    private static async Task<IReadOnlyDictionary<string, DbEnum>> ReadEnumsAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select n.nspname as schema, t.typname as name, e.enumlabel as label
                              from pg_type t
                              join pg_enum e on t.oid = e.enumtypid
                              join pg_namespace n on n.oid = t.typnamespace
                              where n.nspname = 'public'
                              order by n.nspname, t.typname, e.enumsortorder";

        var enums = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(reader.GetOrdinal("name"));
            var label = reader.GetString(reader.GetOrdinal("label"));
            if (!enums.TryGetValue(name, out var list))
            {
                list = new List<string>();
                enums[name] = list;
            }

            list.Add(label);
        }

        return enums.ToDictionary(kv => kv.Key, kv => new DbEnum(kv.Key, kv.Value), StringComparer.OrdinalIgnoreCase);
    }

    private static async Task<IReadOnlyDictionary<string, DbTable>> ReadTablesAsync(NpgsqlConnection conn, IReadOnlyDictionary<string, DbEnum> enums, CancellationToken ct)
    {
        var columns = await ReadColumnsAsync(conn, enums, ct).ConfigureAwait(false);
        var primaryKeys = await ReadPrimaryKeysAsync(conn, ct).ConfigureAwait(false);
        var uniques = await ReadUniqueConstraintsAsync(conn, ct).ConfigureAwait(false);
        var indexes = await ReadIndexesAsync(conn, ct).ConfigureAwait(false);
        var foreignKeys = await ReadForeignKeysAsync(conn, ct).ConfigureAwait(false);

        var groupedColumns = columns.GroupBy(c => c.Table, StringComparer.Ordinal);
        var tables = new Dictionary<string, DbTable>(StringComparer.OrdinalIgnoreCase);
        foreach (var grp in groupedColumns)
        {
            var tableName = grp.Key;
            var cols = grp.ToDictionary(c => c.Column.Name, c => c.Column, StringComparer.Ordinal);

            primaryKeys.TryGetValue(tableName, out var pkCols);
            uniques.TryGetValue(tableName, out var uqList);
            indexes.TryGetValue(tableName, out var idxList);
            foreignKeys.TryGetValue(tableName, out var fkList);

            tables[tableName] = new DbTable(
                tableName,
                cols,
                pkCols ?? new List<string>(),
                uqList ?? new List<DbUnique>(),
                idxList ?? new List<DbIndex>(),
                fkList ?? new List<DbForeignKey>());
        }

        return tables;
    }

    private sealed record ColumnRow(string Table, DbColumn Column);

    private static async Task<List<ColumnRow>> ReadColumnsAsync(NpgsqlConnection conn, IReadOnlyDictionary<string, DbEnum> enums, CancellationToken ct)
    {
        const string sql = @"select
                table_name,
                column_name,
                data_type,
                udt_name,
                is_nullable,
                column_default,
                ordinal_position,
                character_maximum_length
            from information_schema.columns
            where table_schema = 'public'
            order by table_name, ordinal_position";

        var result = new List<ColumnRow>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var column = reader.GetString(reader.GetOrdinal("column_name"));
            var dataType = reader.GetString(reader.GetOrdinal("data_type"));
            var udt = reader.GetString(reader.GetOrdinal("udt_name"));
            var isNullable = reader.GetString(reader.GetOrdinal("is_nullable")) == "YES";
            var defaultValue = reader.IsDBNull(reader.GetOrdinal("column_default"))
                ? null
                : reader.GetString(reader.GetOrdinal("column_default"));
            int? charMaxLength = null;
            if (!reader.IsDBNull(reader.GetOrdinal("character_maximum_length")))
            {
                charMaxLength = reader.GetInt32(reader.GetOrdinal("character_maximum_length"));
            }

            var isEnum = dataType.Equals("USER-DEFINED", StringComparison.OrdinalIgnoreCase) && enums.ContainsKey(udt);
            var resolvedType = ResolveColumnType(dataType, udt, charMaxLength, isEnum);

            result.Add(new ColumnRow(table, new DbColumn(column, resolvedType, isNullable, defaultValue, charMaxLength, false, false, isEnum)));
        }

        return result;
    }

    private static string ResolveColumnType(string dataType, string udt, int? charMaxLength, bool isEnum)
    {
        if (isEnum)
        {
            return udt;
        }

        if (dataType.Equals("character varying", StringComparison.OrdinalIgnoreCase) || dataType.Equals("varchar", StringComparison.OrdinalIgnoreCase))
        {
            return charMaxLength.HasValue ? $"varchar({charMaxLength.Value})" : "varchar";
        }

        if (dataType.Equals("character", StringComparison.OrdinalIgnoreCase) || dataType.Equals("char", StringComparison.OrdinalIgnoreCase))
        {
            return charMaxLength.HasValue ? $"char({charMaxLength.Value})" : "char";
        }

        if (dataType.Equals("timestamp with time zone", StringComparison.OrdinalIgnoreCase)) return "timestamp(3) with time zone";
        if (dataType.Equals("timestamp without time zone", StringComparison.OrdinalIgnoreCase)) return "timestamp(3) without time zone";
        if (dataType.Equals("time with time zone", StringComparison.OrdinalIgnoreCase)) return "time with time zone";
        if (dataType.Equals("time without time zone", StringComparison.OrdinalIgnoreCase)) return "time without time zone";
        if (dataType.Equals("ARRAY", StringComparison.OrdinalIgnoreCase)) return udt; // leave raw for arrays

        return udt;
    }

    private static async Task<Dictionary<string, List<string>>> ReadPrimaryKeysAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select tc.table_name, kcu.column_name
            from information_schema.table_constraints tc
            join information_schema.key_column_usage kcu
              on tc.constraint_name = kcu.constraint_name
             and tc.table_schema = kcu.table_schema
            where tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = 'public'
            order by tc.table_name, kcu.ordinal_position";

        var map = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var column = reader.GetString(reader.GetOrdinal("column_name"));
            if (!map.TryGetValue(table, out var list))
            {
                list = new List<string>();
                map[table] = list;
            }
            list.Add(column);
        }

        return map;
    }

    private static async Task<Dictionary<string, List<DbUnique>>> ReadUniqueConstraintsAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select tc.table_name, tc.constraint_name, kcu.column_name, kcu.ordinal_position
            from information_schema.table_constraints tc
            join information_schema.key_column_usage kcu
              on tc.constraint_name = kcu.constraint_name
             and tc.table_schema = kcu.table_schema
            where tc.constraint_type = 'UNIQUE' and tc.table_schema = 'public'
            order by tc.table_name, tc.constraint_name, kcu.ordinal_position";

        var map = new Dictionary<string, Dictionary<string, List<string>>>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var constraint = reader.GetString(reader.GetOrdinal("constraint_name"));
            var column = reader.GetString(reader.GetOrdinal("column_name"));

            if (!map.TryGetValue(table, out var constraints))
            {
                constraints = new Dictionary<string, List<string>>(StringComparer.Ordinal);
                map[table] = constraints;
            }

            if (!constraints.TryGetValue(constraint, out var cols))
            {
                cols = new List<string>();
                constraints[constraint] = cols;
            }

            cols.Add(column);
        }

        var result = new Dictionary<string, List<DbUnique>>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in map)
        {
            result[kv.Key] = kv.Value.Select(c => new DbUnique(c.Key, c.Value)).ToList();
        }

        return result;
    }

    private static async Task<Dictionary<string, List<DbIndex>>> ReadIndexesAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select
                t.relname as table_name,
                i.relname as index_name,
                idx.indisunique as is_unique,
                idx.indisprimary as is_primary,
                array_agg(a.attname order by k.ordinality) as columns
            from pg_index idx
            join pg_class i on i.oid = idx.indexrelid
            join pg_class t on t.oid = idx.indrelid
            join pg_namespace n on n.oid = t.relnamespace
            left join unnest(idx.indkey) with ordinality as k(attnum, ordinality) on true
            left join pg_attribute a on a.attrelid = t.oid and a.attnum = k.attnum
            where n.nspname = 'public'
            group by t.relname, i.relname, idx.indisunique, idx.indisprimary";

        var result = new Dictionary<string, List<DbIndex>>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var index = reader.GetString(reader.GetOrdinal("index_name"));
            var isUnique = reader.GetBoolean(reader.GetOrdinal("is_unique"));
            var isPrimary = reader.GetBoolean(reader.GetOrdinal("is_primary"));
            var columns = reader.GetFieldValue<string[]>(reader.GetOrdinal("columns"));
            if (columns.Length == 0 || columns.Any(string.IsNullOrEmpty)) continue;

            if (!result.TryGetValue(table, out var list))
            {
                list = new List<DbIndex>();
                result[table] = list;
            }

            list.Add(new DbIndex(index, columns, isUnique, isPrimary));
        }

        return result;
    }

    private static async Task<Dictionary<string, List<DbForeignKey>>> ReadForeignKeysAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select
                                tc.constraint_name,
                                kcu.table_name,
                                kcu.column_name,
                                ccu.table_name as foreign_table_name,
                                ccu.column_name as foreign_column_name,
                                rc.delete_rule,
                                rc.update_rule
                        from information_schema.table_constraints as tc
                        join information_schema.key_column_usage as kcu
                            on tc.constraint_name = kcu.constraint_name
                         and tc.table_schema = kcu.table_schema
                        join information_schema.constraint_column_usage as ccu
                            on ccu.constraint_name = tc.constraint_name
                         and ccu.table_schema = tc.table_schema
                        join information_schema.referential_constraints as rc
                            on rc.constraint_name = tc.constraint_name
                         and rc.constraint_schema = tc.table_schema
                        where tc.constraint_type = 'FOREIGN KEY'
                            and tc.table_schema = 'public'
                        order by kcu.table_name, kcu.column_name";

        var result = new Dictionary<string, List<DbForeignKey>>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var constraint = reader.GetString(reader.GetOrdinal("constraint_name"));
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var column = reader.GetString(reader.GetOrdinal("column_name"));
            var refTable = reader.GetString(reader.GetOrdinal("foreign_table_name"));
            var refColumn = reader.GetString(reader.GetOrdinal("foreign_column_name"));
            var deleteRule = reader.GetString(reader.GetOrdinal("delete_rule"));
            var updateRule = reader.GetString(reader.GetOrdinal("update_rule"));

            if (!result.TryGetValue(table, out var list))
            {
                list = new List<DbForeignKey>();
                result[table] = list;
            }

            // If composite FKs are needed, this should aggregate by constraint; for now treat single-column FKs.
            list.Add(new DbForeignKey(constraint, new[] { column }, refTable, new[] { refColumn }, deleteRule, updateRule));
        }

        return result;
    }
}
