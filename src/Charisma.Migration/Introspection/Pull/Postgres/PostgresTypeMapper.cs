using System;
using System.Collections.Generic;
using Charisma.Schema;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Maps Postgres column metadata to schema field definitions.
/// </summary>
internal static class PostgresTypeMapper
{
    public static FieldDefinition Map(ColumnInfo column)
    {
        var rawType = MapRawType(column);
        var attributes = new List<string>();
        var defaultKind = MapDefault(column, out var defaultValue);
        var dbAttribute = MapDbNativeAttribute(column);
        var isUpdatedAt = IsUpdatedAtColumn(column);

        if (isUpdatedAt)
        {
            attributes.Add("@updatedAt");
            defaultKind = null;
        }

        if (column.IsPrimaryKey)
        {
            attributes.Add("@id");
        }
        else if (column.IsUnique)
        {
            attributes.Add("@unique");
        }

        if (defaultKind is not null)
        {
            attributes.Add(BuildDefaultAttribute(defaultKind.Value, defaultValue));
        }

        if (dbAttribute is not null)
        {
            attributes.Add(dbAttribute);
        }

        // For now we only emit scalars; relations will be inferred by higher-level diffing if needed.
        return new ScalarFieldDefinition(
            name: IdentifierCasing.ToFieldName(column.ColumnName),
            rawType: rawType,
            isList: false,
            isOptional: column.IsNullable && !column.IsPrimaryKey,
            attributes: attributes,
            isId: column.IsPrimaryKey,
            isUnique: column.IsUnique,
            isUpdatedAt: isUpdatedAt || IsUpdatedAtDefault(defaultKind),
            defaultValue: defaultKind is null ? null : new DefaultValueDefinition(defaultKind.Value, defaultValue));
    }

    private static string MapRawType(ColumnInfo column)
    {
        // User-defined types (enums) map to PascalCase enum names
        if (string.Equals(column.PgDataType, "USER-DEFINED", StringComparison.OrdinalIgnoreCase))
        {
            return IdentifierCasing.ToEnumName(column.UdType);
        }

        var udt = column.UdType.ToLowerInvariant();
        return udt switch
        {
            "uuid" => GuessId(column) ? "Id" : "Uuid",
            "jsonb" or "json" => "Json",
            "timestamp" or "timestamptz" => "DateTime",
            "time" or "timetz" => "DateTime",
            "date" => "DateTime",
            "int2" => "Int",
            "int4" => "Int",
            "int8" => "BigInt",
            "float4" or "float8" => "Float",
            "numeric" => "Decimal",
            "bool" => "Boolean",
            "text" or "varchar" or "bpchar" or "citext" => "String",
            "bytea" => "Bytes",
            _ => IdentifierCasing.Pascalize(column.UdType)
        };
    }

    private static string? MapDbNativeAttribute(ColumnInfo column)
    {
        var udt = column.UdType.ToLowerInvariant();

        if (string.Equals(column.PgDataType, "USER-DEFINED", StringComparison.OrdinalIgnoreCase))
        {
            return null; // enums use their mapped name without @db
        }

        return udt switch
        {
            "uuid" => "@db.Uuid",
            "varchar" => BuildSizedAttribute("@db.VarChar", column.CharacterMaximumLength),
            "bpchar" => BuildSizedAttribute("@db.Char", column.CharacterMaximumLength),
            "text" => "@db.Text",
            "citext" => "@db.Citext",
            "bytea" => "@db.ByteA",
            _ => null
        };
    }

    private static string BuildSizedAttribute(string attribute, int? length)
    {
        if (length.HasValue && length.Value > 0)
        {
            return $"{attribute}({length.Value})";
        }

        return attribute;
    }

    private static bool GuessId(ColumnInfo column)
    {
        return column.ColumnName.EndsWith("id", StringComparison.OrdinalIgnoreCase);
    }

    private static DefaultValueKind? MapDefault(ColumnInfo column, out string? value)
    {
        value = null;
        if (string.IsNullOrEmpty(column.DefaultValue))
        {
            return null;
        }

        var def = column.DefaultValue;
        if (def.Contains("nextval", StringComparison.OrdinalIgnoreCase))
        {
            return DefaultValueKind.Autoincrement;
        }

        if (def.Contains("uuid_generate_v4", StringComparison.OrdinalIgnoreCase))
        {
            return DefaultValueKind.UuidV4;
        }

        if (def.Contains("gen_random_uuid", StringComparison.OrdinalIgnoreCase))
        {
            return DefaultValueKind.UuidV4;
        }

        if (def.Equals("now()", StringComparison.OrdinalIgnoreCase) || def.Contains("now()"))
        {
            return DefaultValueKind.Now;
        }

        if (def.StartsWith("'", StringComparison.Ordinal))
        {
            var secondQuote = def.IndexOf('\'', 1);
            if (secondQuote > 1)
            {
                value = def.Substring(1, secondQuote - 1);
                return DefaultValueKind.Static;
            }

            if (def.EndsWith("'", StringComparison.Ordinal))
            {
                value = def.Trim('\'', ' ');
                return DefaultValueKind.Static;
            }
        }

        return null;
    }

    private static bool IsUpdatedAtColumn(ColumnInfo column)
    {
        var name = column.ColumnName.ToLowerInvariant();
        return name == "updated_at" || name == "updatedat" || name.EndsWith("updated_at") || name.EndsWith("updatedat");
    }

    private static bool IsUpdatedAtDefault(DefaultValueKind? kind)
    {
        return kind == DefaultValueKind.Now;
    }

    private static string BuildDefaultAttribute(DefaultValueKind kind, string? value)
    {
        return kind switch
        {
            DefaultValueKind.Autoincrement => "@default(autoincrement())",
            DefaultValueKind.UuidV4 => "@default(uuid())",
            DefaultValueKind.UuidV7 => "@default(uuid())",
            DefaultValueKind.Now => "@default(now())",
            DefaultValueKind.Json when value is not null => $"@default({value})",
            DefaultValueKind.Static when value is not null => $"@default(\"{value}\")",
            _ => "@default()"
        };
    }
}
