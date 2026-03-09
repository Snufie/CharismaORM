using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Charisma.Schema;

namespace Charisma.Migration.Introspection.Push.Postgres;

/// <summary>
/// Shared SQL helpers for Postgres push/diff.
/// </summary>
internal static class PostgresSchemaPusherHelpers
{
    public static string BuildEnum(EnumDefinition enumDef)
    {
        var typeName = ToPgEnumTypeName(enumDef.Name);
        var values = string.Join(", ", enumDef.Values.Select(v => $"'{v}'"));
        // Some Postgres versions do not support "create type if not exists"; drop defensively, then recreate.
        return $"drop type if exists \"{typeName}\" cascade; create type \"{typeName}\" as enum ({values});";
    }

    public static string BuildTable(ModelDefinition model, IReadOnlyDictionary<string, EnumDefinition> enums)
    {
        var sb = new StringBuilder();
        sb.Append("create table if not exists \"").Append(model.Name).Append("\" (\n");

        var columnLines = new List<string>();
        foreach (var field in model.Fields.OfType<ScalarFieldDefinition>())
        {
            columnLines.Add("  " + BuildColumn(field, enums.ContainsKey(field.RawType)));
        }

        if (model.PrimaryKey is not null && model.PrimaryKey.Fields.Count > 0)
        {
            var pkCols = string.Join(", ", model.PrimaryKey.Fields.Select(Quote));
            columnLines.Add($"  constraint \"{model.Name}_pkey\" primary key ({pkCols})");
        }

        foreach (var uq in model.UniqueConstraints)
        {
            var name = string.IsNullOrEmpty(uq.Name) ? $"{model.Name}_uq_{string.Join("_", uq.Fields)}" : uq.Name!;
            var cols = string.Join(", ", uq.Fields.Select(Quote));
            columnLines.Add($"  constraint \"{name}\" unique ({cols})");
        }

        sb.Append(string.Join(",\n", columnLines));
        sb.Append("\n);");
        return sb.ToString();
    }

    public static string BuildAddColumn(string table, ScalarFieldDefinition field, bool isEnum)
    {
        var col = BuildColumn(field, isEnum);
        return $"alter table \"{table}\" add column \n  {col};";
    }

    public static string BuildIndex(string table, IndexDefinition idx)
    {
        var name = string.IsNullOrEmpty(idx.Name) ? $"idx_{table}_{string.Join("_", idx.Fields)}" : idx.Name!;
        var cols = string.Join(", ", idx.Fields.Select(Quote));
        var unique = idx.IsUnique ? "unique " : string.Empty;
        return $"create {unique}index if not exists \"{name}\" on \"{table}\" ({cols});";
    }

    public static string BuildDropIndex(string table, string indexName)
    {
        return $"drop index if exists \"{indexName}\";";
    }

    public static string BuildForeignKey(string table, RelationInfo ri)
    {
        var localCols = string.Join(", ", ri.LocalFields.Select(Quote));
        var foreignCols = string.Join(", ", ri.ForeignFields.Select(Quote));
        var constraintName = $"{table}_{string.Join("_", ri.LocalFields)}_fkey";
        var onDelete = MapAction(ri.OnDelete) ?? "set null";
        var onUpdate = MapAction(ri.OnUpdate) ?? "cascade";

        return $"alter table \"{table}\" add constraint \"{constraintName}\" foreign key ({localCols}) references \"{ri.ForeignModel}\" ({foreignCols}) on delete {onDelete} on update {onUpdate};";
    }

    public static string BuildColumn(ScalarFieldDefinition field, bool isEnum)
    {
        var type = MapColumnType(field, isEnum);
        var nullability = field.IsOptional ? "" : " not null";
        var defaultSql = field.IsUpdatedAt ? " default now()" : MapDefault(field, isEnum);
        return $"\"{field.Name}\" {type}{defaultSql}{nullability}";
    }

    public static string BuildAlterColumnType(string table, string column, string targetType, bool withUsingCast = false, string? usingExpression = null)
    {
        var usingClause = usingExpression ?? (withUsingCast ? $" using \"{column}\"::{targetType}" : string.Empty);
        return $"alter table \"{table}\" alter column \"{column}\" type {targetType}{usingClause};";
    }

    public static string BuildAlterNullability(string table, string column, bool notNull)
    {
        return notNull
            ? $"alter table \"{table}\" alter column \"{column}\" set not null;"
            : $"alter table \"{table}\" alter column \"{column}\" drop not null;";
    }

    public static string BuildSetDefault(string table, string column, string defaultExpression)
    {
        return $"alter table \"{table}\" alter column \"{column}\" set default {defaultExpression};";
    }

    public static string BuildDropDefault(string table, string column)
    {
        return $"alter table \"{table}\" alter column \"{column}\" drop default;";
    }

    public static string BuildDropColumn(string table, string column)
    {
        return $"alter table \"{table}\" drop column if exists \"{column}\" cascade;";
    }

    public static string BuildDropTable(string table)
    {
        return $"drop table if exists \"{table}\" cascade;";
    }

    public static string BuildDropInboundForeignKeys(string table, string column)
    {
        return $@"do $$
declare r record;
begin
    for r in (
        select tc.constraint_name, tc.table_name
        from information_schema.table_constraints tc
        join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name and tc.constraint_schema = kcu.constraint_schema
        join information_schema.constraint_column_usage ccu on tc.constraint_name = ccu.constraint_name and tc.constraint_schema = ccu.constraint_schema
        where tc.constraint_type = 'FOREIGN KEY'
            and ccu.table_schema = 'public'
            and kcu.table_schema = 'public'
            and ccu.table_name = '{table}'
            and ccu.column_name = '{column}'
    ) loop
        execute format('alter table %I drop constraint %I', r.table_name, r.constraint_name);
    end loop;
end$$;";
    }

    public static string BuildRenameColumn(string table, string from, string to)
    {
        return $"alter table \"{table}\" rename column \"{from}\" to \"{to}\";";
    }

    public static string MapColumnType(ScalarFieldDefinition field, bool isEnum)
    {
        if (isEnum)
        {
            var typeName = ToPgEnumTypeName(field.RawType);
            return $"\"{typeName}\"{(field.IsList ? "[]" : string.Empty)}";
        }

        var attrType = ResolveDbTypeAttribute(field.Attributes ?? Array.Empty<string>());
        var baseType = attrType ?? ResolveBaseType(field);

        if (field.IsList)
        {
            return baseType + "[]";
        }

        return baseType;
    }

    public static string? ResolveDbTypeAttribute(IReadOnlyList<string> attributes)
    {
        foreach (var attr in attributes)
        {
            if (attr.StartsWith("@db.VarChar", StringComparison.OrdinalIgnoreCase))
            {
                var len = ExtractLength(attr);
                return len.HasValue ? $"varchar({len.Value})" : "varchar";
            }

            if (attr.StartsWith("@db.Char", StringComparison.OrdinalIgnoreCase))
            {
                var len = ExtractLength(attr);
                return len.HasValue ? $"char({len.Value})" : "char";
            }

            if (attr.Equals("@db.Text", StringComparison.OrdinalIgnoreCase)) return "text";
            if (attr.Equals("@db.Citext", StringComparison.OrdinalIgnoreCase)) return "citext";
            if (attr.Equals("@db.ByteA", StringComparison.OrdinalIgnoreCase)) return "bytea";
            if (attr.Equals("@db.Uuid", StringComparison.OrdinalIgnoreCase)) return "uuid";
            if (attr.Equals("@db.Timestamp", StringComparison.OrdinalIgnoreCase)) return "timestamp(3) without time zone";
            if (attr.Equals("@db.Timestamptz", StringComparison.OrdinalIgnoreCase)) return "timestamp(3) with time zone";
            if (attr.Equals("@db.Time", StringComparison.OrdinalIgnoreCase)) return "time without time zone";
            if (attr.Equals("@db.Timetz", StringComparison.OrdinalIgnoreCase)) return "time with time zone";
            if (attr.Equals("@db.Date", StringComparison.OrdinalIgnoreCase)) return "date";
            if (attr.Equals("@db.Decimal", StringComparison.OrdinalIgnoreCase)) return "numeric";
            if (attr.Equals("@db.Integer", StringComparison.OrdinalIgnoreCase)) return "integer";
            if (attr.Equals("@db.SmallInt", StringComparison.OrdinalIgnoreCase)) return "smallint";
            if (attr.Equals("@db.BigInt", StringComparison.OrdinalIgnoreCase)) return "bigint";
            if (attr.Equals("@db.Real", StringComparison.OrdinalIgnoreCase)) return "real";
            if (attr.Equals("@db.DoublePrecision", StringComparison.OrdinalIgnoreCase)) return "double precision";
            if (attr.Equals("@db.Boolean", StringComparison.OrdinalIgnoreCase)) return "boolean";
        }

        return null;
    }

    public static string MapDefault(ScalarFieldDefinition field, bool isEnum)
    {
        if (field.DefaultValue is null) return string.Empty;

        var def = field.DefaultValue;
        return def.Kind switch
        {
            DefaultValueKind.Autoincrement when field.RawType is "Int" or "BigInt" => string.Empty,
            DefaultValueKind.UuidV4 or DefaultValueKind.UuidV7 => " default gen_random_uuid()",
            DefaultValueKind.Now => " default now()",
            DefaultValueKind.Json when def.Value is not null => $" default '{EscapeSqlLiteral(def.Value)}'::jsonb",
            DefaultValueKind.Static when def.Value is not null => BuildStaticDefault(def.Value, field.RawType, isEnum),
            _ => string.Empty
        };
    }

    public static string BuildStaticDefault(string value, string rawType, bool isEnum)
    {
        if (isEnum)
        {
            var typeName = ToPgEnumTypeName(rawType);
            return $" default '{EscapeSqlLiteral(value)}'::\"{typeName}\"";
        }

        var lowered = value.Trim().ToLowerInvariant();
        if (lowered is "now()" or "now(")
        {
            return " default now()";
        }

        return rawType switch
        {
            "Boolean" => $" default {(value.Equals("true", StringComparison.OrdinalIgnoreCase) ? "true" : "false")}",
            "Int" or "BigInt" or "Float" or "Decimal" => $" default {value}",
            _ => $" default '{EscapeSqlLiteral(value)}'"
        };
    }

    public static string ResolveBaseType(ScalarFieldDefinition field)
    {
        var type = ResolveBaseType(field.RawType);
        if (field.DefaultValue?.Kind == DefaultValueKind.Autoincrement)
        {
            if (field.RawType == "Int") return "serial";
            if (field.RawType == "BigInt") return "bigserial";
        }
        return type;
    }

    public static string ResolveBaseType(string rawType)
    {
        return rawType switch
        {
            "String" => "text",
            "Boolean" => "boolean",
            "Int" => "integer",
            "BigInt" => "bigint",
            "Float" => "double precision",
            "Decimal" => "numeric",
            "DateTime" => "timestamp(3) without time zone",
            "Json" => "jsonb",
            "Bytes" => "bytea",
            "Uuid" or "Id" => "uuid",
            _ => "text"
        };
    }

    public static string Quote(string identifier) => $"\"{identifier}\"";

    public static int? ExtractLength(string attr)
    {
        var start = attr.IndexOf('(');
        var end = attr.IndexOf(')');
        if (start < 0 || end <= start + 1) return null;
        var span = attr.Substring(start + 1, end - start - 1);
        if (int.TryParse(span, out var len)) return len;
        return null;
    }

    public static string EscapeSqlLiteral(string input) => input.Replace("'", "''");

    private static string ToPgEnumTypeName(string clrType)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < clrType.Length; i++)
        {
            var c = clrType[i];
            if (char.IsUpper(c) && i > 0)
            {
                sb.Append('_');
            }
            sb.Append(char.ToLowerInvariant(c));
        }
        return sb.ToString();
    }

    public static string? MapAction(OnDeleteBehavior? behavior)
    {
        return behavior switch
        {
            OnDeleteBehavior.Cascade => "cascade",
            OnDeleteBehavior.SetNull => "set null",
            OnDeleteBehavior.Restrict => "restrict",
            OnDeleteBehavior.NoAction => "no action",
            null => null,
            _ => null
        };
    }
}