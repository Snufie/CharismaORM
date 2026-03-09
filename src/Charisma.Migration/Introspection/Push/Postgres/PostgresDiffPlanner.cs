using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;
using Charisma.Migration.Postgres;
using System.Collections.ObjectModel;

namespace Charisma.Migration.Introspection.Push.Postgres;

/// <summary>
/// Computes a diff between desired schema and current Postgres snapshot, classifying warnings and unexecutable changes.
/// </summary>
internal sealed class PostgresDiffPlanner
{
    private readonly PostgresDatabaseIntrospector _introspector;
    private readonly PostgresMigrationOptions _options;
    private readonly DropSafetyChecker _safetyChecker;

    public PostgresDiffPlanner(PostgresDatabaseIntrospector introspector, PostgresMigrationOptions options, DropSafetyChecker safetyChecker)
    {
        _introspector = introspector ?? throw new ArgumentNullException(nameof(introspector));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _safetyChecker = safetyChecker ?? throw new ArgumentNullException(nameof(safetyChecker));
    }

    public async Task<MigrationPlan> PlanAsync(CharismaSchema desired, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(desired);
        var current = await _introspector.ReadAsync(cancellationToken).ConfigureAwait(false);

        var steps = new List<MigrationStep>();
        var warnings = new List<string>();
        var unexecutable = new List<string>();

        // Drop enums that no longer exist
        foreach (var enumName in current.Enums.Keys.Except(desired.Enums.Keys, StringComparer.OrdinalIgnoreCase))
        {
            var sql = $"drop type if exists \"{enumName}\" cascade;";
            steps.Add(new MigrationStep($"Drop enum {enumName}", true, sql));
        }

        // Enums
        foreach (var enumDef in desired.Enums.Values)
        {
            if (!current.Enums.ContainsKey(enumDef.Name))
            {
                var sql = PostgresSchemaPusherHelpers.BuildEnum(enumDef);
                steps.Add(new MigrationStep($"Create enum {enumDef.Name}", false, sql));
            }
        }

        // Tables & columns
        foreach (var model in desired.Models.Values)
        {
            if (!current.Tables.TryGetValue(model.Name, out var existingTable))
            {
                var sql = PostgresSchemaPusherHelpers.BuildTable(model, desired.Enums);
                steps.Add(new MigrationStep($"Create table {model.Name}", false, sql));
                continue;
            }

            await PlanColumnDifferencesAsync(model, existingTable, desired.Enums, steps, warnings, unexecutable, cancellationToken).ConfigureAwait(false);

            // Add missing scalar columns
            foreach (var field in model.Fields.OfType<ScalarFieldDefinition>())
            {
                if (existingTable.Columns.ContainsKey(field.Name)) continue;
                var sql = PostgresSchemaPusherHelpers.BuildAddColumn(model.Name, field, desired.Enums.ContainsKey(field.RawType));
                steps.Add(new MigrationStep($"Add column {model.Name}.{field.Name}", false, sql));
            }

            // Add missing uniques (@@unique or single-field with name)
            var existingUniqueSets = new HashSet<string>(existingTable.Uniques.Select(u => string.Join("|", u.Columns)), StringComparer.Ordinal);
            foreach (var uq in model.UniqueConstraints)
            {
                var key = string.Join("|", uq.Fields);
                if (existingUniqueSets.Contains(key)) continue;
                var name = string.IsNullOrEmpty(uq.Name) ? $"{model.Name}_uq_{string.Join("_", uq.Fields)}" : uq.Name!;
                var cols = string.Join(", ", uq.Fields.Select(PostgresSchemaPusherHelpers.Quote));
                var sql = $"alter table \"{model.Name}\" add constraint \"{name}\" unique ({cols});";
                steps.Add(new MigrationStep($"Add unique on {model.Name} ({string.Join(", ", uq.Fields)})", false, sql));
            }

            // Add missing indexes (@@index non-unique)
            var existingIndexes = new HashSet<string>(existingTable.Indexes.Where(i => !i.IsUnique && !i.IsPrimary).Select(i => string.Join("|", i.Columns)), StringComparer.Ordinal);
            foreach (var idx in model.Indexes.Where(i => !i.IsUnique))
            {
                var key = string.Join("|", idx.Fields);
                if (existingIndexes.Contains(key)) continue;
                var sql = PostgresSchemaPusherHelpers.BuildIndex(model.Name, idx);
                steps.Add(new MigrationStep($"Add index on {model.Name} ({string.Join(", ", idx.Fields)})", false, sql));
            }

            // Add missing FKs
            var existingFks = new HashSet<string>(existingTable.ForeignKeys.Select(fk => string.Join("|", fk.LocalColumns) + "->" + fk.ForeignTable + "|" + string.Join("|", fk.ForeignColumns)), StringComparer.Ordinal);
            foreach (var rel in model.Fields.OfType<RelationFieldDefinition>())
            {
                if (rel.RelationInfo is null || rel.RelationInfo.IsCollection) continue;
                var ri = rel.RelationInfo;
                var key = string.Join("|", ri.LocalFields) + "->" + ri.ForeignModel + "|" + string.Join("|", ri.ForeignFields);
                if (existingFks.Contains(key)) continue;
                var sql = PostgresSchemaPusherHelpers.BuildForeignKey(model.Name, ri);
                steps.Add(new MigrationStep($"Add foreign key on {model.Name} ({string.Join(", ", ri.LocalFields)}) -> {ri.ForeignModel} ({string.Join(", ", ri.ForeignFields)})", false, sql));
            }
        }

        // Drop tables that disappeared
        foreach (var table in current.Tables.Keys.Except(desired.Models.Keys, StringComparer.OrdinalIgnoreCase))
        {
            var sql = PostgresSchemaPusherHelpers.BuildDropTable(table);
            steps.Add(new MigrationStep($"Drop table {table}", true, sql));
            var isEmpty = await _safetyChecker.IsTableEmptyAsync(table, cancellationToken).ConfigureAwait(false);
            if (!isEmpty)
            {
                warnings.Add($"Dropping table {table} will delete data.");
            }

            var hasRefs = await _safetyChecker.TableHasReferencingRowsAsync(table, cancellationToken).ConfigureAwait(false);
            if (hasRefs)
            {
                unexecutable.Add($"Table {table} is referenced by other tables with existing data; drop is blocked.");
            }

            var hasInboundFks = await _safetyChecker.TableHasInboundForeignKeysAsync(table, cancellationToken).ConfigureAwait(false);
            if (hasInboundFks)
            {
                unexecutable.Add($"Table {table} has inbound foreign key constraints; drop is blocked unless reset.");
            }
        }

        // Add/ensure foreign keys for all relations (including newly created tables)
        var existingFkKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var tbl in current.Tables.Values)
        {
            foreach (var fk in tbl.ForeignKeys)
            {
                var key = BuildFkKey(tbl.Name, fk.LocalColumns, fk.ForeignTable, fk.ForeignColumns);
                existingFkKeys.Add(key);
            }
        }

        foreach (var model in desired.Models.Values)
        {
            foreach (var rel in model.Fields.OfType<RelationFieldDefinition>())
            {
                if (rel.RelationInfo is null || rel.RelationInfo.IsCollection) continue;
                var ri = rel.RelationInfo;
                var key = BuildFkKey(model.Name, ri.LocalFields, ri.ForeignModel, ri.ForeignFields);
                if (existingFkKeys.Contains(key)) continue;

                var fkSql = PostgresSchemaPusherHelpers.BuildForeignKey(model.Name, ri);
                steps.Add(new MigrationStep($"Add foreign key on {model.Name} ({string.Join(", ", ri.LocalFields)}) -> {ri.ForeignModel} ({string.Join(", ", ri.ForeignFields)})", false, fkSql));

                // Add an index on FK columns to aid performance
                if (ri.LocalFields.Count > 0)
                {
                    var idxName = $"idx_{model.Name}_{string.Join("_", ri.LocalFields)}_fk";
                    var cols = string.Join(", ", ri.LocalFields.Select(PostgresSchemaPusherHelpers.Quote));
                    var idxSql = $"create index if not exists \"{idxName}\" on \"{model.Name}\" ({cols});";
                    steps.Add(new MigrationStep($"Add index for FK {model.Name} ({string.Join(", ", ri.LocalFields)})", false, idxSql));
                }
            }
        }

        return new MigrationPlan(steps, warnings, unexecutable);
    }

    private static string BuildFkKey(string table, IReadOnlyList<string> localCols, string foreignTable, IReadOnlyList<string> foreignCols)
    {
        return $"{table}|{string.Join("|", localCols)}->{foreignTable}|{string.Join("|", foreignCols)}";
    }

    private async Task PlanColumnDifferencesAsync(
        ModelDefinition model,
        DbTable existingTable,
        IReadOnlyDictionary<string, EnumDefinition> enums,
        List<MigrationStep> steps,
        List<string> warnings,
        List<string> unexecutable,
        CancellationToken cancellationToken)
    {
        var desiredScalars = model.Fields.OfType<ScalarFieldDefinition>().ToList();
        var desiredNames = new HashSet<string>(desiredScalars.Select(f => f.Name), StringComparer.Ordinal);
        var existingNames = new HashSet<string>(existingTable.Columns.Keys, StringComparer.Ordinal);

        // Rename detection: attempt to map missing to extra columns using type/nullability/default similarity (one-to-one)
        if (_options.AllowRenames)
        {
            var missing = desiredNames.Except(existingNames, StringComparer.Ordinal).ToList();
            var extra = existingNames.Except(desiredNames, StringComparer.Ordinal).ToList();
            var matchedExtras = new HashSet<string>(StringComparer.Ordinal);

            foreach (var missingName in missing)
            {
                var desiredField = desiredScalars.First(f => f.Name == missingName);
                var desiredType = PostgresSchemaPusherHelpers.MapColumnType(desiredField, enums.ContainsKey(desiredField.RawType));
                var desiredNotNull = !desiredField.IsOptional;
                var desiredDefault = NormalizeDesiredDefault(PostgresSchemaPusherHelpers.MapDefault(desiredField, enums.ContainsKey(desiredField.RawType)));

                var candidates = extra
                    .Where(e => !matchedExtras.Contains(e))
                    .Select(e => (Name: e, Column: existingTable.Columns[e]))
                    .Where(e => string.Equals(desiredType, NormalizeType(e.Column), StringComparison.OrdinalIgnoreCase))
                    .Select(e => new
                    {
                        e.Name,
                        Score = ScoreRename(desiredNotNull, desiredDefault, e.Column)
                    })
                    .OrderByDescending(c => c.Score)
                    .ToList();

                if (candidates.Count == 1 || (candidates.Count > 1 && candidates[0].Score > candidates[1].Score))
                {
                    var from = candidates[0].Name;
                    var sql = PostgresSchemaPusherHelpers.BuildRenameColumn(model.Name, from, desiredField.Name);
                    steps.Add(new MigrationStep($"Rename column {model.Name}.{from} to {desiredField.Name}", false, sql));
                    matchedExtras.Add(from);
                    existingNames.Remove(from);
                    existingNames.Add(desiredField.Name);
                }
            }
        }

        // Drops
        foreach (var col in existingTable.Columns)
        {
            if (desiredNames.Contains(col.Key)) continue;
            var sql = PostgresSchemaPusherHelpers.BuildDropColumn(existingTable.Name, col.Key);
            steps.Add(new MigrationStep($"Drop column {model.Name}.{col.Key}", true, sql));
            var isEmpty = await _safetyChecker.IsTableEmptyAsync(existingTable.Name, cancellationToken).ConfigureAwait(false);
            if (!isEmpty)
            {
                warnings.Add($"Dropping column {model.Name}.{col.Key} will delete data.");
            }

            // Block drops that participate in foreign keys unless user resets
            var participatesInLocalFk = existingTable.ForeignKeys.Any(fk => fk.LocalColumns.Contains(col.Key, StringComparer.Ordinal));
            if (participatesInLocalFk)
            {
                var dropLocalFkSql = PostgresSchemaPusherHelpers.BuildDropInboundForeignKeys(existingTable.Name, col.Key);
                steps.Add(new MigrationStep($"Drop inbound/outbound FKs referencing {model.Name}.{col.Key}", true, dropLocalFkSql));
            }

            var hasInboundRefs = await _safetyChecker.ColumnHasInboundReferencesAsync(existingTable.Name, col.Key, cancellationToken).ConfigureAwait(false);
            if (hasInboundRefs)
            {
                warnings.Add($"Column {model.Name}.{col.Key} is referenced by other tables with data; drop will remove referenced values.");
            }

            var hasInboundFks = await _safetyChecker.ColumnHasInboundForeignKeysAsync(existingTable.Name, col.Key, cancellationToken).ConfigureAwait(false);
            if (hasInboundFks)
            {
                var dropInboundFkSql = PostgresSchemaPusherHelpers.BuildDropInboundForeignKeys(existingTable.Name, col.Key);
                steps.Add(new MigrationStep($"Drop inbound FKs referencing {model.Name}.{col.Key}", true, dropInboundFkSql));
            }
        }

        // Alters
        foreach (var field in desiredScalars)
        {
            if (!existingTable.Columns.TryGetValue(field.Name, out var existing))
            {
                continue;
            }

            var desiredType = NormalizeTypeName(PostgresSchemaPusherHelpers.MapColumnType(field, enums.ContainsKey(field.RawType)));
            var currentType = NormalizeTypeName(NormalizeType(existing));

            if (!string.Equals(desiredType, currentType, StringComparison.OrdinalIgnoreCase))
            {
                var isEnumChange = desiredType.StartsWith("\"") && currentType.StartsWith("\"");
                if (isEnumChange)
                {
                    var usingExpr = $" using \"{field.Name}\"::text::{desiredType}";
                    steps.Add(new MigrationStep($"Alter column type {model.Name}.{field.Name} to {desiredType}", true, PostgresSchemaPusherHelpers.BuildAlterColumnType(model.Name, field.Name, desiredType, usingExpression: usingExpr)));
                }
                else
                {
                    var compatibility = ClassifyTypeChange(currentType, desiredType);
                    switch (compatibility)
                    {
                        case TypeChangeClassification.Unsafe:
                            unexecutable.Add($"Changing type of {model.Name}.{field.Name} from {currentType} to {desiredType} is not safely castable.");
                            break;
                        case TypeChangeClassification.Warning:
                            warnings.Add($"Changing type of {model.Name}.{field.Name} from {currentType} to {desiredType} may be destructive.");
                            steps.Add(new MigrationStep($"Alter column type {model.Name}.{field.Name} to {desiredType}", true, PostgresSchemaPusherHelpers.BuildAlterColumnType(model.Name, field.Name, desiredType, withUsingCast: true)));
                            break;
                        case TypeChangeClassification.Safe:
                            steps.Add(new MigrationStep($"Alter column type {model.Name}.{field.Name} to {desiredType}", false, PostgresSchemaPusherHelpers.BuildAlterColumnType(model.Name, field.Name, desiredType, withUsingCast: true)));
                            break;
                    }
                }
            }

            var shouldBeNotNull = !field.IsOptional;
            if (existing.IsNullable != shouldBeNotNull)
            {
                var sql = PostgresSchemaPusherHelpers.BuildAlterNullability(existingTable.Name, field.Name, shouldBeNotNull);
                if (shouldBeNotNull)
                {
                    var hasNulls = await _safetyChecker.ColumnHasNullsAsync(existingTable.Name, field.Name, cancellationToken).ConfigureAwait(false);
                    if (hasNulls)
                    {
                        unexecutable.Add($"Column {model.Name}.{field.Name} contains NULLs; cannot make it required.");
                    }
                }
                steps.Add(new MigrationStep($"Alter nullability {model.Name}.{field.Name} {(shouldBeNotNull ? "set" : "drop")} not null", shouldBeNotNull, sql));
            }

            if (field.DefaultValue?.Kind != DefaultValueKind.Autoincrement)
            {
                var desiredDefault = NormalizeDesiredDefault(PostgresSchemaPusherHelpers.MapDefault(field, enums.ContainsKey(field.RawType)));
                var currentDefault = NormalizeExistingDefault(existing.DefaultValue);

                if (!string.Equals(desiredDefault, currentDefault, StringComparison.OrdinalIgnoreCase))
                {
                    if (desiredDefault is null)
                    {
                        var sql = PostgresSchemaPusherHelpers.BuildDropDefault(existingTable.Name, field.Name);
                        steps.Add(new MigrationStep($"Drop default {model.Name}.{field.Name}", false, sql));
                    }
                    else
                    {
                        var sql = PostgresSchemaPusherHelpers.BuildSetDefault(existingTable.Name, field.Name, desiredDefault);
                        steps.Add(new MigrationStep($"Set default {model.Name}.{field.Name}", false, sql));
                    }
                }
            }
        }
    }

    private static string NormalizeType(DbColumn col)
    {
        if (col.IsEnum)
        {
            return $"\"{col.DataType}\"";
        }
        var aliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["int4"] = "integer",
            ["serial"] = "integer",
            ["serial4"] = "integer",
            ["int8"] = "bigint",
            ["serial8"] = "bigint",
            ["bigserial"] = "bigint",
            ["bool"] = "boolean",
            ["float4"] = "real",
            ["float8"] = "double precision",
            ["timestamp without time zone"] = "timestamp(3) without time zone",
            ["timestamp with time zone"] = "timestamp(3) with time zone",
            ["time without time zone"] = "time without time zone",
            ["time with time zone"] = "time with time zone",
        };

        if (aliases.TryGetValue(col.DataType, out var canonical))
        {
            return canonical;
        }

        return col.DataType;
    }

    private static string NormalizeTypeName(string type)
    {
        if (string.IsNullOrWhiteSpace(type)) return type;
        if (type.StartsWith("\"")) return type; // leave enum/type names alone

        var aliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["int4"] = "integer",
            ["serial"] = "integer",
            ["serial4"] = "integer",
            ["int8"] = "bigint",
            ["serial8"] = "bigint",
            ["bigserial"] = "bigint",
            ["bool"] = "boolean",
            ["float4"] = "real",
            ["float8"] = "double precision",
            ["timestamp without time zone"] = "timestamp(3) without time zone",
            ["timestamp with time zone"] = "timestamp(3) with time zone",
            ["time without time zone"] = "time without time zone",
            ["time with time zone"] = "time with time zone",
        };

        return aliases.TryGetValue(type, out var canonical) ? canonical : type;
    }

    private static string? NormalizeDesiredDefault(string defaultSql)
    {
        if (string.IsNullOrWhiteSpace(defaultSql)) return null;
        // defaultSql is " default expr" per MapDefault
        var trimmed = defaultSql.Trim();
        return trimmed.StartsWith("default ", StringComparison.OrdinalIgnoreCase)
            ? trimmed.Substring("default ".Length).Trim()
            : trimmed;
    }

    private static string? NormalizeExistingDefault(string? defaultValue)
    {
        if (string.IsNullOrWhiteSpace(defaultValue)) return null;
        return defaultValue.Trim();
    }

    private static int ScoreRename(bool desiredNotNull, string? desiredDefault, DbColumn existing)
    {
        var score = 0;
        if (existing.IsNullable == !desiredNotNull) score += 1;
        var existingDefault = NormalizeExistingDefault(existing.DefaultValue);
        if (string.Equals(desiredDefault, existingDefault, StringComparison.OrdinalIgnoreCase)) score += 1;
        return score;
    }

    private enum TypeChangeClassification
    {
        Safe,
        Warning,
        Unsafe
    }

    private static TypeChangeClassification ClassifyTypeChange(string currentType, string desiredType)
    {
        // Normalize to lower for comparison
        var from = currentType.ToLowerInvariant();
        var to = desiredType.ToLowerInvariant();

        if (from == to) return TypeChangeClassification.Safe;

        // Widening numeric
        if (from == "integer" && (to == "bigint" || to == "numeric" || to == "double precision")) return TypeChangeClassification.Safe;
        if (from == "smallint" && (to == "integer" || to == "bigint" || to == "numeric" || to == "double precision")) return TypeChangeClassification.Safe;
        if (from == "bigint" && to == "numeric") return TypeChangeClassification.Safe;

        // Textual widenings
        if (from.StartsWith("varchar") && (to == "text" || to.StartsWith("varchar") || to.StartsWith("citext"))) return TypeChangeClassification.Safe;
        if (from.StartsWith("char") && (to.StartsWith("varchar") || to == "text")) return TypeChangeClassification.Safe;
        if (from == "text" && (to == "citext" || to.StartsWith("varchar"))) return TypeChangeClassification.Warning;

        // Date/Time
        if (from == "timestamp(3) without time zone" && to == "timestamp(3) with time zone") return TypeChangeClassification.Warning;
        if (from == "timestamp(3) with time zone" && to == "timestamp(3) without time zone") return TypeChangeClassification.Warning;

        // JSON to text or vice versa
        if ((from == "jsonb" || from == "json") && (to == "text" || to.StartsWith("varchar"))) return TypeChangeClassification.Warning;

        // Enum rename or rebrand
        if (from.StartsWith("\"") && to.StartsWith("\"")) return TypeChangeClassification.Warning;
        // Enum to text
        if (from.StartsWith("\"") && to == "text") return TypeChangeClassification.Warning;

        // Anything else: unsafe
        return TypeChangeClassification.Unsafe;
    }
}
