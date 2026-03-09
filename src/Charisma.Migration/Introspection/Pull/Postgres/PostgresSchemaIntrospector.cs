using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;
using Npgsql;

namespace Charisma.Migration.Postgres;

/// <summary>
/// Placeholder for Postgres catalog introspection to produce a <see cref="CharismaSchema"/>.
/// </summary>
public sealed class PostgresSchemaIntrospector : ISchemaIntrospector
{
    private readonly PostgresIntrospectionOptions _options;
    private readonly CharismaSchema? _existing;

    public PostgresSchemaIntrospector(PostgresIntrospectionOptions options, CharismaSchema? existing = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _existing = existing;
    }

    public async Task<CharismaSchema> IntrospectAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var enums = await ReadEnumsAsync(conn, cancellationToken).ConfigureAwait(false);
        var columns = await ReadColumnsAsync(conn, cancellationToken).ConfigureAwait(false);
        var foreignKeys = await ReadForeignKeysAsync(conn, cancellationToken).ConfigureAwait(false);
        var primaryKeys = await ReadPrimaryKeysAsync(conn, cancellationToken).ConfigureAwait(false);
        var uniqueConstraints = await ReadUniqueConstraintsAsync(conn, cancellationToken).ConfigureAwait(false);
        var indexes = await ReadIndexesAsync(conn, cancellationToken).ConfigureAwait(false);

        // Mark PK/Unique on columns without mutating during enumeration
        var columnList = columns.ToList();
        var pkSet = new HashSet<string>(primaryKeys.SelectMany(pk => pk.Columns.Select(col => Key(pk.Table, col))), StringComparer.Ordinal);
        var uniqueSet = new HashSet<string>(uniqueConstraints.SelectMany(uq => uq.Columns.Select(col => Key(uq.Table, col))), StringComparer.Ordinal);

        for (int i = 0; i < columnList.Count; i++)
        {
            var col = columnList[i];
            if (pkSet.Contains(Key(col.TableName, col.ColumnName)))
            {
                col = col with { IsPrimaryKey = true };
            }
            else if (uniqueSet.Contains(Key(col.TableName, col.ColumnName)))
            {
                col = col with { IsUnique = true };
            }

            columnList[i] = col;
        }

        var models = BuildModels(columnList, enums, foreignKeys, uniqueConstraints, indexes);
        var datasources = BuildDatasource();
        var generators = BuildGenerator();

        var schema = new CharismaSchema(
            models,
            enums.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal),
            datasources,
            generators);

        return _existing is null ? schema : ApplyExistingNames(schema, _existing);
    }

    private static CharismaSchema ApplyExistingNames(CharismaSchema schema, CharismaSchema existing)
    {
        var existingModels = existing.Models.Values.ToDictionary(m => m.Name, m => m, StringComparer.OrdinalIgnoreCase);
        var existingEnums = existing.Enums.Values.ToDictionary(e => e.Name, e => e, StringComparer.OrdinalIgnoreCase);

        // Enums: if names match ignoring case, reuse existing name
        var remappedEnums = new Dictionary<string, EnumDefinition>(StringComparer.Ordinal);
        foreach (var enumDef in schema.Enums.Values)
        {
            if (existingEnums.TryGetValue(enumDef.Name, out var existingEnum))
            {
                remappedEnums[existingEnum.Name] = new EnumDefinition(existingEnum.Name, enumDef.Values);
            }
            else
            {
                remappedEnums[enumDef.Name] = enumDef;
            }
        }

        var remappedModels = new Dictionary<string, ModelDefinition>(StringComparer.Ordinal);
        foreach (var model in schema.Models.Values)
        {
            var targetModelName = model.Name;
            Dictionary<string, FieldDefinition>? existingFields = null;

            if (existingModels.TryGetValue(model.Name, out var existingModel))
            {
                targetModelName = existingModel.Name;
                existingFields = existingModel.Fields.ToDictionary(f => f.Name, f => f, StringComparer.OrdinalIgnoreCase);
            }

            // Remap fields
            var remappedFields = new List<FieldDefinition>();
            foreach (var field in model.Fields)
            {
                var newName = MapField(existingFields, field.Name);
                var newRawType = remappedEnums.TryGetValue(field.RawType, out var enumDef) ? enumDef.Name : MapModel(existingModels, field.RawType);

                if (field is RelationFieldDefinition rel)
                {
                    RelationInfo? ri = null;
                    if (rel.RelationInfo is not null)
                    {
                        ri = new RelationInfo(
                            foreignModel: MapModel(existingModels, rel.RelationInfo.ForeignModel),
                            localFields: rel.RelationInfo.LocalFields.Select(f => MapField(existingFields, f)).ToList(),
                            foreignFields: rel.RelationInfo.ForeignFields.Select(f => MapField(existingModels.TryGetValue(rel.RelationInfo.ForeignModel, out var em) ? em.Fields.ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase) : null, f)).ToList(),
                            isCollection: rel.RelationInfo.IsCollection,
                            relationName: rel.RelationInfo.RelationName,
                            onDelete: rel.RelationInfo.OnDelete);
                    }

                    remappedFields.Add(new RelationFieldDefinition(
                        name: newName,
                        rawType: MapModel(existingModels, rel.RawType),
                        isList: rel.IsList,
                        isOptional: rel.IsOptional,
                        attributes: rel.Attributes,
                        relationAttributes: rel.RelationAttributes,
                        relationInfo: ri));
                }
                else if (field is ScalarFieldDefinition scalar)
                {
                    remappedFields.Add(new ScalarFieldDefinition(
                        name: newName,
                        rawType: newRawType,
                        isList: scalar.IsList,
                        isOptional: scalar.IsOptional,
                        attributes: scalar.Attributes,
                        isId: scalar.IsId,
                        isUnique: scalar.IsUnique,
                        isUpdatedAt: scalar.IsUpdatedAt,
                        defaultValue: scalar.DefaultValue));
                }
                else
                {
                    remappedFields.Add(field);
                }
            }

            // Remap PK/unique
            PrimaryKeyDefinition? remappedPk = null;
            if (model.PrimaryKey is not null)
            {
                remappedPk = new PrimaryKeyDefinition(model.PrimaryKey.Fields.Select(f => MapField(existingFields, f)).ToList(), model.PrimaryKey.Name);
            }

            var remappedUniques = model.UniqueConstraints.Select(uq => new UniqueConstraintDefinition(uq.Fields.Select(f => MapField(existingFields, f)).ToList(), uq.Name)).ToList();

            remappedModels[targetModelName] = new ModelDefinition(targetModelName, remappedFields, model.Attributes, remappedPk, remappedUniques, model.Indexes);
        }

        return new CharismaSchema(remappedModels, remappedEnums, schema.Datasources.ToList(), schema.Generators.ToList());
    }

    private static string MapField(Dictionary<string, FieldDefinition>? existingFields, string name)
    {
        if (existingFields is null) return name;
        return existingFields.TryGetValue(name, out var f) ? f.Name : name;
    }

    private static string MapModel(Dictionary<string, ModelDefinition> existingModels, string name)
    {
        return existingModels.TryGetValue(name, out var m) ? m.Name : name;
    }
    private static async Task<IReadOnlyDictionary<string, EnumDefinition>> ReadEnumsAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select n.nspname as schema, t.typname as name, e.enumlabel as label
                              from pg_type t
                              join pg_enum e on t.oid = e.enumtypid
                              join pg_namespace n on n.oid = t.typnamespace
                              order by n.nspname, t.typname, e.enumsortorder";

        var enums = new Dictionary<string, List<string>>(StringComparer.Ordinal);

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

        return enums.ToDictionary(
                kv => IdentifierCasing.ToEnumName(kv.Key),
                kv => (EnumDefinition)new EnumDefinition(IdentifierCasing.ToEnumName(kv.Key), kv.Value),
            StringComparer.Ordinal);
    }

    private static async Task<IReadOnlyList<ForeignKeyInfo>> ReadForeignKeysAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select
                                tc.constraint_name,
                                kcu.table_name,
                                kcu.column_name,
                                ccu.table_name as foreign_table_name,
                                ccu.column_name as foreign_column_name,
                                cols.is_nullable,
                                rc.delete_rule
                        from information_schema.table_constraints as tc
                        join information_schema.key_column_usage as kcu
                            on tc.constraint_name = kcu.constraint_name
                         and tc.table_schema = kcu.table_schema
                        join information_schema.constraint_column_usage as ccu
                            on ccu.constraint_name = tc.constraint_name
                         and ccu.table_schema = tc.table_schema
                        join information_schema.columns as cols
                            on cols.table_name = kcu.table_name
                         and cols.column_name = kcu.column_name
                        join information_schema.referential_constraints as rc
                            on rc.constraint_name = tc.constraint_name
                         and rc.constraint_schema = tc.table_schema
                        where tc.constraint_type = 'FOREIGN KEY'
                            and tc.table_schema = 'public'
                        order by kcu.table_name, kcu.column_name";

        var result = new List<ForeignKeyInfo>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var constraint = reader.GetString(reader.GetOrdinal("constraint_name"));
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var column = reader.GetString(reader.GetOrdinal("column_name"));
            var refTable = reader.GetString(reader.GetOrdinal("foreign_table_name"));
            var refColumn = reader.GetString(reader.GetOrdinal("foreign_column_name"));
            var isNullable = reader.GetString(reader.GetOrdinal("is_nullable")) == "YES";
            var deleteRule = reader.GetString(reader.GetOrdinal("delete_rule"));

            result.Add(new ForeignKeyInfo(table, column, refTable, refColumn, constraint, isNullable, deleteRule));
        }

        return result;
    }

    private static async Task<List<ColumnInfo>> ReadColumnsAsync(NpgsqlConnection conn, CancellationToken ct)
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

        var result = new List<ColumnInfo>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var column = reader.GetString(reader.GetOrdinal("column_name"));
            var dataType = reader.GetString(reader.GetOrdinal("data_type"));
            var udt = reader.GetString(reader.GetOrdinal("udt_name"));
            var isNullable = reader.GetString(reader.GetOrdinal("is_nullable")) == "YES";
            var ordinal = reader.GetInt32(reader.GetOrdinal("ordinal_position"));
            int? charMaxLength = null;
            if (!reader.IsDBNull(reader.GetOrdinal("character_maximum_length")))
            {
                charMaxLength = reader.GetInt32(reader.GetOrdinal("character_maximum_length"));
            }

            string? defaultValue = null;
            if (!reader.IsDBNull(reader.GetOrdinal("column_default")))
            {
                defaultValue = reader.GetString(reader.GetOrdinal("column_default"));
            }

            result.Add(new ColumnInfo(table, column, dataType, udt, isNullable, defaultValue, IsPrimaryKey: false, IsUnique: false, Ordinal: ordinal, CharacterMaximumLength: charMaxLength));
        }

        return result;
    }

    private static async Task<List<(string Table, List<string> Columns)>> ReadPrimaryKeysAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select tc.table_name, kcu.column_name
            from information_schema.table_constraints tc
            join information_schema.key_column_usage kcu
              on tc.constraint_name = kcu.constraint_name
             and tc.table_schema = kcu.table_schema
            where tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = 'public'
            order by tc.table_name, kcu.ordinal_position";

        var map = new Dictionary<string, List<string>>(StringComparer.Ordinal);
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

        return map.Select(kv => (kv.Key, kv.Value)).ToList();
    }

    private static async Task<List<IndexInfo>> ReadIndexesAsync(NpgsqlConnection conn, CancellationToken ct)
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

        var result = new List<IndexInfo>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var index = reader.GetString(reader.GetOrdinal("index_name"));
            var isUnique = reader.GetBoolean(reader.GetOrdinal("is_unique"));
            var isPrimary = reader.GetBoolean(reader.GetOrdinal("is_primary"));
            var columns = reader.GetFieldValue<string[]>(reader.GetOrdinal("columns"));
            if (columns.Length == 0 || columns.Any(string.IsNullOrEmpty))
            {
                continue; // skip expression/invalid indexes
            }

            result.Add(new IndexInfo(table, index, isUnique, isPrimary, columns));
        }

        return result;
    }

    private static async Task<List<(string Table, string Constraint, List<string> Columns)>> ReadUniqueConstraintsAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        const string sql = @"select tc.table_name, tc.constraint_name, kcu.column_name, kcu.ordinal_position
            from information_schema.table_constraints tc
            join information_schema.key_column_usage kcu
              on tc.constraint_name = kcu.constraint_name
             and tc.table_schema = kcu.table_schema
            where tc.constraint_type = 'UNIQUE' and tc.table_schema = 'public'
            order by tc.table_name, tc.constraint_name, kcu.ordinal_position";

        var map = new Dictionary<(string Table, string Constraint), List<string>>();
        await using var cmd = new NpgsqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var table = reader.GetString(reader.GetOrdinal("table_name"));
            var constraint = reader.GetString(reader.GetOrdinal("constraint_name"));
            var column = reader.GetString(reader.GetOrdinal("column_name"));
            var key = (table, constraint);
            if (!map.TryGetValue(key, out var list))
            {
                list = new List<string>();
                map[key] = list;
            }
            list.Add(column);
        }

        return map.Select(kv => (kv.Key.Table, kv.Key.Constraint, kv.Value)).ToList();
    }

    private static Dictionary<string, ModelDefinition> BuildModels(
        IEnumerable<ColumnInfo> columns,
        IReadOnlyDictionary<string, EnumDefinition> enums,
        IReadOnlyList<ForeignKeyInfo> foreignKeys,
        IReadOnlyList<(string Table, string Constraint, List<string> Columns)> uniqueConstraints,
        IReadOnlyList<IndexInfo> indexes)
    {
        var models = new Dictionary<string, ModelDefinition>(StringComparer.Ordinal);
        var grouped = columns.GroupBy(c => c.TableName, StringComparer.Ordinal);
        var collectionRelations = new Dictionary<string, List<RelationFieldDefinition>>(StringComparer.Ordinal);

        foreach (var table in grouped)
        {
            var fields = new List<FieldDefinition>();
            var attributes = new List<string>();
            PrimaryKeyDefinition? pk = null;
            var uniques = new List<UniqueConstraintDefinition>();

            var columnToFieldName = table.ToDictionary(c => c.ColumnName, c => IdentifierCasing.ToFieldName(c.ColumnName), StringComparer.OrdinalIgnoreCase);

            foreach (var column in table.OrderBy(c => c.Ordinal))
            {
                fields.Add(PostgresTypeMapper.Map(column));
            }

            // Add navigation relation fields from FKs: one-to-one/many-to-one side.
            foreach (var fk in foreignKeys.Where(f => f.TableName == table.Key))
            {
                var relationName = DeriveRelationName(fk);
                var relationInfo = new RelationInfo(
                    foreignModel: IdentifierCasing.ToModelName(fk.ReferencedTable),
                    localFields: new[] { columnToFieldName[fk.ColumnName] },
                    foreignFields: new[] { IdentifierCasing.ToFieldName(fk.ReferencedColumn) },
                    isCollection: false,
                    relationName: null,
                    onDelete: MapOnDelete(fk.DeleteRule),
                    onUpdate: OnDeleteBehavior.Cascade);

                fields.Add(new RelationFieldDefinition(
                    name: relationName,
                    rawType: IdentifierCasing.ToModelName(fk.ReferencedTable),
                    isList: false,
                    isOptional: fk.IsNullable,
                    attributes: Array.Empty<string>(),
                    relationAttributes: Array.Empty<string>(),
                    relationInfo: relationInfo));

                // Track collection side for the referenced model
                var referencedModelName = IdentifierCasing.ToModelName(fk.ReferencedTable);
                if (!collectionRelations.TryGetValue(referencedModelName, out var list))
                {
                    list = new List<RelationFieldDefinition>();
                    collectionRelations[referencedModelName] = list;
                }

                list.Add(new RelationFieldDefinition(
                    name: DeriveCollectionName(table.Key),
                    rawType: IdentifierCasing.ToModelName(table.Key),
                    isList: true,
                    isOptional: false,
                    attributes: Array.Empty<string>(),
                    relationAttributes: Array.Empty<string>(),
                    relationInfo: null));
            }

            var pkCols = table.Where(c => c.IsPrimaryKey).OrderBy(c => c.Ordinal).Select(c => columnToFieldName[c.ColumnName]).ToList();
            if (pkCols.Count == 1)
            {
                // Single-field PK is already annotated on the field; still store the typed PK
                pk = new PrimaryKeyDefinition(pkCols);
            }
            else if (pkCols.Count > 1)
            {
                pk = new PrimaryKeyDefinition(pkCols);
            }

            foreach (var uq in uniqueConstraints.Where(u => u.Table == table.Key))
            {
                var mapped = uq.Columns.Select(c => columnToFieldName[c]).ToList();
                uniques.Add(new UniqueConstraintDefinition(mapped, uq.Constraint));
            }

            var tableIndexes = indexes.Where(ix => ix.TableName == table.Key && !ix.IsUnique && !ix.IsPrimary).ToList();
            var indexDefs = tableIndexes.Select(ix => new IndexDefinition(ix.Columns.Select(c => columnToFieldName[c]).ToList(), isUnique: false, name: ix.IndexName)).ToList();

            var modelName = IdentifierCasing.ToModelName(table.Key);
            models[modelName] = new ModelDefinition(modelName, fields, attributes, pk, uniques, indexes: indexDefs);
        }

        // Attach collection-side fields after base models exist
        foreach (var kv in collectionRelations)
        {
            if (!models.TryGetValue(kv.Key, out var model)) continue;
            var extendedFields = model.Fields.Concat(kv.Value).ToList();
            models[kv.Key] = new ModelDefinition(model.Name, extendedFields, model.Attributes, model.PrimaryKey, model.UniqueConstraints, model.Indexes);
        }

        return models;
    }

    private static string Key(string table, string column) => table + "\u0001" + column;

    private static string DeriveCollectionName(string sourceTable)
    {
        return IdentifierCasing.ToFieldName(sourceTable);
    }

    private static string DeriveRelationName(ForeignKeyInfo fk)
    {
        // If FK column ends with "Id" or "ID", drop that suffix for the navigation name; otherwise use referenced table name.
        var col = fk.ColumnName;
        if (col.EndsWith("id", StringComparison.OrdinalIgnoreCase))
        {
            var trimmed = col[..^2];
            return string.IsNullOrWhiteSpace(trimmed) ? IdentifierCasing.ToFieldName(fk.ReferencedTable) : IdentifierCasing.ToFieldName(trimmed);
        }

        return IdentifierCasing.ToFieldName(fk.ReferencedTable);
    }

    private static OnDeleteBehavior MapOnDelete(string rule)
    {
        var deleteRule = rule?.ToUpperInvariant() ?? string.Empty;
        return deleteRule switch
        {
            "CASCADE" => OnDeleteBehavior.Cascade,
            "SET NULL" => OnDeleteBehavior.SetNull,
            "RESTRICT" => OnDeleteBehavior.Restrict,
            "NO ACTION" => OnDeleteBehavior.NoAction,
            _ => OnDeleteBehavior.Cascade // default to Prisma's default onUpdate/onDelete when unspecified
        };
    }

    private List<DatasourceDefinition> BuildDatasource()
    {
        var options = new Dictionary<string, string>(StringComparer.Ordinal);

        // Preserve connection string as raw url expression (env("DATABASE_URL") style if passed that way).
        var url = _options.ConnectionString.StartsWith("env(", StringComparison.OrdinalIgnoreCase)
            ? _options.ConnectionString
            : "\"" + _options.ConnectionString + "\"";

        var datasource = new DatasourceDefinition(
            name: _options.DatasourceName,
            provider: "postgresql",
            url: url,
            options: options);

        return new List<DatasourceDefinition> { datasource };
    }

    private List<GeneratorDefinition> BuildGenerator()
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "provider", _options.GeneratorProvider }
        };

        if (!string.IsNullOrEmpty(_options.GeneratorOutput))
        {
            config["output"] = _options.GeneratorOutput!;
        }

        var generator = new GeneratorDefinition(_options.GeneratorName, config);
        return new List<GeneratorDefinition> { generator };
    }
}
