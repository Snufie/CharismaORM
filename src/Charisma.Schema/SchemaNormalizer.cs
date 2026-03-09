using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Charisma.Schema;

namespace Charisma.Schema
{
    /// <summary>
    /// Produces a canonical, deterministic textual representation of a <see cref="CharismaSchema"/>.
    /// This representation is used exclusively for schema hashing and generator determinism.
    ///
    /// Normalization rules:
    /// - Stable ordering of all elements
    /// - No comments
    /// - No insignificant whitespace
    /// - Prisma-like DSL shape
    /// - Assumes the schema is already valid
    ///
    /// This type must remain pure and side-effect free.
    /// </summary>
    public static class SchemaNormalizer
    {
        /// <summary>
        /// Normalizes the given <see cref="CharismaSchema"/> into a canonical string.
        /// </summary>
        public static string Normalize(CharismaSchema schema)
        {
            if (schema is null)
                throw new ArgumentNullException(nameof(schema));

            var sb = new StringBuilder();

            AppendDatasources(schema, sb);
            AppendGenerators(schema, sb);
            AppendEnums(schema, sb);
            AppendModels(schema, sb);

            return sb.ToString().TrimEnd();
        }

        private static void AppendDatasources(CharismaSchema schema, StringBuilder sb)
        {
            foreach (var datasource in schema.Datasources)
            {
                sb.Append("datasource ");
                sb.Append(datasource.Name);
                sb.AppendLine(" {");

                sb.Append("  provider = \"");
                sb.Append(datasource.Provider);
                sb.AppendLine("\"");

                sb.Append("  url = ");
                sb.AppendLine(datasource.Url);

                foreach (var option in datasource.Options.OrderBy(kv => kv.Key, StringComparer.Ordinal))
                {
                    sb.Append("  ");
                    sb.Append(option.Key);
                    sb.Append(" = \"");
                    sb.Append(option.Value);
                    sb.AppendLine("\"");
                }

                sb.AppendLine("}");
                sb.AppendLine();
            }
        }

        private static void AppendGenerators(CharismaSchema schema, StringBuilder sb)
        {
            foreach (var generator in schema.Generators)
            {
                sb.Append("generator ");
                sb.Append(generator.Name);
                sb.AppendLine(" {");

                foreach (var kv in generator.Config.OrderBy(kv => kv.Key, StringComparer.Ordinal))
                {
                    sb.Append("  ");
                    sb.Append(kv.Key);
                    sb.Append(" = \"");
                    sb.Append(kv.Value);
                    sb.AppendLine("\"");
                }

                sb.AppendLine("}");
                sb.AppendLine();
            }
        }

        private static void AppendEnums(CharismaSchema schema, StringBuilder sb)
        {
            foreach (var enumDef in schema.Enums.Values.OrderBy(e => e.Name, StringComparer.Ordinal))
            {
                sb.Append("enum ");
                sb.Append(enumDef.Name);
                sb.AppendLine(" {");

                foreach (var value in enumDef.Values)
                {
                    sb.Append("  ");
                    sb.AppendLine(value);
                }

                sb.AppendLine("}");
                sb.AppendLine();
            }
        }

        private static void AppendModels(CharismaSchema schema, StringBuilder sb)
        {
            foreach (var model in schema.Models.Values.OrderBy(m => m.Name, StringComparer.Ordinal))
            {
                sb.Append("model ");
                sb.Append(model.Name);
                sb.AppendLine(" {");

                var fields = model.Fields
                    .OrderBy(f => f.Name, StringComparer.Ordinal)
                    .ToList();

                var maxName = fields.Max(f => f.Name.Length);
                var maxType = fields.Max(f => NormalizeFieldType(f).Length);

                foreach (var field in fields)
                {
                    var typeText = NormalizeFieldType(field);

                    sb.Append("  ");
                    sb.Append(field.Name.PadRight(maxName + 1));
                    sb.Append(typeText.PadRight(maxType + 1));

                    // Attributes (preserve given order) plus relation attribute if applicable
                    foreach (var attr in field.Attributes)
                    {
                        sb.Append(' ');
                        sb.Append(attr);
                    }

                    if (field is RelationFieldDefinition rel)
                    {
                        var relationAttr = BuildRelationAttribute(model.Name, rel);
                        if (!string.IsNullOrEmpty(relationAttr))
                        {
                            sb.Append(' ');
                            sb.Append(relationAttr);
                        }
                    }

                    sb.AppendLine();
                }

                // Model-level attributes (e.g. @@index)
                foreach (var attr in model.Attributes)
                {
                    sb.Append("  ");
                    sb.AppendLine(attr);
                }

                sb.AppendLine("}");
                sb.AppendLine();
            }
        }

        private static string NormalizeFieldType(FieldDefinition field)
        {
            var typeName = field.RawType;

            if (field.IsList)
                typeName += "[]";

            if (field.IsOptional)
                typeName += "?";

            return typeName;
        }

        private static string? BuildRelationAttribute(string localModelName, RelationFieldDefinition rel)
        {
            // Prefer typed RelationInfo; fall back to raw relation attributes if provided
            if (rel.RelationInfo is not null)
            {
                var info = rel.RelationInfo;
                var parts = new List<string>();

                if (info.LocalFields.Count > 0)
                {
                    parts.Add($"fk: [{string.Join(", ", info.LocalFields.Select(f => $"{localModelName}.{f}"))}]");
                }

                if (info.ForeignFields.Count > 0)
                {
                    parts.Add($"pk: [{string.Join(", ", info.ForeignFields.Select(f => $"{info.ForeignModel}.{f}"))}]");
                }

                if (!string.IsNullOrWhiteSpace(info.RelationName))
                {
                    parts.Add($"name: \"{info.RelationName}\"");
                }

                if (info.OnDelete != OnDeleteBehavior.SetNull)
                {
                    parts.Add($"onDelete: {info.OnDelete}");
                }

                if (parts.Count == 0) return null;
                return "@relation(" + string.Join(", ", parts) + ")";
            }

            if (rel.RelationAttributes.Count > 0)
            {
                var raw = string.Join(", ", rel.RelationAttributes.Where(r => !string.IsNullOrWhiteSpace(r)));
                if (raw.Length == 0) return null;
                return "@relation(" + raw + ")";
            }

            return null;
        }
    }

    public static class SchemaHasher
    {
        /// <summary>
        /// Computes a SHA256 hash of the normalized schema.
        /// </summary>
        public static string Compute(string normalizedText)
        {
            using var sha256 = System.Security.Cryptography.SHA256.Create();
            var bytes = Encoding.UTF8.GetBytes(normalizedText);
            var hashBytes = sha256.ComputeHash(bytes);
            return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
        }
    }
}
