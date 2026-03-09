using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Npgsql;

namespace Charisma.QueryEngine.Execution;

public sealed partial class PostgresSqlExecutor
{
    private static bool FkAllowsNullOnLocal(ModelMetadata owningMeta, ForeignKeyMetadata fk)
    {
        foreach (var localField in fk.LocalFields)
        {
            var field = owningMeta.Fields.FirstOrDefault(f => string.Equals(f.Name, localField, StringComparison.Ordinal)) ?? throw new InvalidOperationException($"Field '{localField}' not found on '{owningMeta.Name}'.");
            if (!field.IsNullable)
            {
                return false;
            }
        }

        return true;
    }

    private static (Dictionary<string, object?> Values, bool HasNull) ExtractForeignKeyValuesFromParent(ModelMetadata parentMeta, ForeignKeyMetadata fk, object parentInstance)
    {
        var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
        var parentType = parentInstance.GetType();
        var hasNull = false;

        for (var i = 0; i < fk.LocalFields.Count; i++)
        {
            var localField = fk.LocalFields[i];
            var principalField = fk.PrincipalFields[i];
            var prop = parentType.GetProperty(localField, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase) ?? throw new InvalidOperationException($"Property '{localField}' not found on '{parentType.Name}'.");
            var value = prop.GetValue(parentInstance);
            if (value is null)
            {
                hasNull = true;
            }

            dict[principalField] = value;
        }

        return (dict, hasNull);
    }

    private static void AppendParentPkPredicate(ModelMetadata parentMeta, IReadOnlyDictionary<string, object?> parentPk, string alias, ParameterContext pctx, List<NpgsqlParameter> parameters, List<string> predicates)
    {
        if (parentMeta.PrimaryKey is null)
        {
            throw new InvalidOperationException($"Model '{parentMeta.Name}' does not have a primary key.");
        }

        for (var i = 0; i < parentMeta.PrimaryKey.Fields.Count; i++)
        {
            var pkField = parentMeta.PrimaryKey.Fields[i];
            if (!parentPk.TryGetValue(pkField, out var value))
            {
                throw new InvalidOperationException($"Primary key value for field '{pkField}' is missing on '{parentMeta.Name}'.");
            }

            var paramName = pctx.Next();
            parameters.Add(new NpgsqlParameter(paramName, value ?? DBNull.Value));
            predicates.Add($"\"{alias}\".{QuoteIdentifier(pkField)} = {paramName}");
        }
    }

    private static void AppendParentFkAssignmentsFromChildPk(ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> childPk, ParameterContext pctx, List<NpgsqlParameter> parameters, List<string> assignments, bool setNull = false)
    {
        for (var i = 0; i < fk.LocalFields.Count; i++)
        {
            var localField = fk.LocalFields[i];
            var principalField = fk.PrincipalFields[i];
            object? value = null;

            if (!setNull)
            {
                if (!childPk.TryGetValue(principalField, out value))
                {
                    throw new InvalidOperationException($"Primary key value for field '{principalField}' is missing on child entity.");
                }
            }

            var paramName = pctx.Next();
            parameters.Add(new NpgsqlParameter(paramName, value ?? DBNull.Value));
            assignments.Add($"{QuoteIdentifier(localField)} = {paramName}");
        }
    }

    private static void AppendChildPkPredicate(ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> childPk, string alias, ParameterContext pctx, List<NpgsqlParameter> parameters, List<string> predicates)
    {
        for (var i = 0; i < fk.PrincipalFields.Count; i++)
        {
            var principalField = fk.PrincipalFields[i];
            if (!childPk.TryGetValue(principalField, out var value))
            {
                throw new InvalidOperationException($"Primary key value for field '{principalField}' is missing on child entity.");
            }

            var paramName = pctx.Next();
            parameters.Add(new NpgsqlParameter(paramName, value ?? DBNull.Value));
            predicates.Add($"\"{alias}\".{QuoteIdentifier(principalField)} = {paramName}");
        }
    }

    private async Task SetParentForeignKeyAsync(ModelMetadata parentMeta, ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk, IReadOnlyDictionary<string, object?> childPk, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        var parameters = new List<NpgsqlParameter>();
        var pctx = new ParameterContext();
        var assignments = new List<string>();
        AppendParentFkAssignmentsFromChildPk(fk, childPk, pctx, parameters, assignments);

        var predicates = new List<string>();
        AppendParentPkPredicate(parentMeta, parentPk, "p", pctx, parameters, predicates);

        var sql = new StringBuilder();
        sql.Append("UPDATE ").Append(QuoteIdentifier(parentMeta.Name)).Append(" AS \"p\" SET ").Append(string.Join(", ", assignments));
        if (predicates.Count > 0)
        {
            sql.Append(" WHERE ").Append(string.Join(" AND ", predicates));
        }

        var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, parentMeta.Name).ConfigureAwait(false);
        if (affected == 0)
        {
            throw new RecordNotFoundException(parentMeta.Name, $"Update.{parentMeta.Name}");
        }
    }

    private static void ApplyParentForeignKeyLocally(ModelMetadata parentMeta, ForeignKeyMetadata fk, ModelMetadata childMeta, object parentInstance, IReadOnlyDictionary<string, object?> childPk, bool setNull = false)
    {
        var principalFields = fk.PrincipalFields.Count > 0
            ? fk.PrincipalFields
            : childMeta.PrimaryKey?.Fields
              ?? throw new InvalidOperationException($"Foreign key '{fk.RelationName ?? childMeta.Name}' missing principal fields and '{childMeta.Name}' has no primary key.");

        for (int i = 0; i < fk.LocalFields.Count; i++)
        {
            var local = fk.LocalFields[i];
            object? value = null;

            if (!setNull)
            {
                var principal = principalFields[i];
                if (!childPk.TryGetValue(principal, out value))
                {
                    throw new InvalidOperationException($"Primary key value for field '{principal}' is missing on child entity '{childMeta.Name}'.");
                }
            }

            var prop = parentInstance.GetType().GetProperty(local, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (prop?.CanWrite == true)
            {
                prop.SetValue(parentInstance, value);
            }
        }
    }

    private object? CreateChildPlaceholderInstance(ModelMetadata childMeta, IReadOnlyDictionary<string, object?> childPk)
    {
        var childType = _modelTypeResolver(childMeta.Name);
        var childInstance = Activator.CreateInstance(childType);
        if (childInstance is null)
        {
            return null;
        }

        if (childMeta.PrimaryKey is { Fields.Count: > 0 })
        {
            foreach (var pkField in childMeta.PrimaryKey.Fields)
            {
                if (!childPk.TryGetValue(pkField, out var value))
                {
                    continue;
                }

                var pkProp = childType.GetProperty(pkField, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                if (pkProp?.CanWrite == true)
                {
                    pkProp.SetValue(childInstance, value);
                }
            }
        }

        return childInstance;
    }

    private void ApplyParentRelationPlaceholder(ModelMetadata childMeta, FieldMetadata relationField, object parentInstance, IReadOnlyDictionary<string, object?> childPk)
    {
        // Placeholder hydration is disabled; includes should be used to fetch related entities explicitly.
        return;
    }

    private bool CollectionContainsChildPk(ModelMetadata childMeta, System.Collections.IEnumerable collection, IReadOnlyDictionary<string, object?> childPk)
    {
        if (childMeta.PrimaryKey is not { Fields.Count: > 0 })
        {
            return false;
        }

        foreach (var item in collection)
        {
            if (item is null)
            {
                continue;
            }

            var itemType = item.GetType();
            var matches = true;
            foreach (var pkField in childMeta.PrimaryKey.Fields)
            {
                var pkProp = itemType.GetProperty(pkField, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                var current = pkProp?.GetValue(item);
                childPk.TryGetValue(pkField, out var desired);
                if (!Equals(current, desired))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                return true;
            }
        }

        return false;
    }

    private void AddChildPlaceholderToCollection(ModelMetadata childMeta, FieldMetadata relationField, object parentInstance, IReadOnlyDictionary<string, object?> childPk)
    {
        // Placeholder hydration is disabled; includes should be used to fetch related entities explicitly.
        return;
    }

    private void ApplyParentOwnedCreatePlaceholders(ModelMetadata parentMeta, IReadOnlyDictionary<FieldMetadata, Dictionary<string, object?>?> resolved, object parentInstance)
    {
        // Placeholder hydration is disabled; includes should be used to fetch related entities explicitly.
        return;
    }

    private static void ClearParentRelation(object parentInstance, FieldMetadata relationField)
    {
        var prop = parentInstance.GetType().GetProperty(relationField.Name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (prop?.CanWrite == true)
        {
            prop.SetValue(parentInstance, null);
        }
    }

    private async Task ClearParentForeignKeyAsync(ModelMetadata parentMeta, ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        if (!FkAllowsNullOnLocal(parentMeta, fk))
        {
            throw new NotSupportedException("Relation cannot clear because the parent foreign key is non-nullable.");
        }

        var parameters = new List<NpgsqlParameter>();
        var pctx = new ParameterContext();
        var assignments = new List<string>();
        AppendParentFkAssignmentsFromChildPk(fk, new Dictionary<string, object?>(StringComparer.Ordinal), pctx, parameters, assignments, setNull: true);

        var predicates = new List<string>();
        AppendParentPkPredicate(parentMeta, parentPk, "p", pctx, parameters, predicates);

        var sql = new StringBuilder();
        sql.Append("UPDATE ").Append(QuoteIdentifier(parentMeta.Name)).Append(" AS \"p\" SET ").Append(string.Join(", ", assignments));
        if (predicates.Count > 0)
        {
            sql.Append(" WHERE ").Append(string.Join(" AND ", predicates));
        }

        await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, parentMeta.Name).ConfigureAwait(false);
    }

    private async Task<Dictionary<string, object?>> FetchChildPkByWhereAsync(ModelMetadata childMeta, object whereObj, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct, string relationName)
    {
        var parameters = new List<NpgsqlParameter>();
        var pctx = new ParameterContext();
        var whereSql = BuildWhereUnique(childMeta, whereObj, parameters, pctx, "c");

        var selectColumns = BuildSelectColumns(childMeta, null, includePrimaryKey: true);

        var sql = new StringBuilder();
        sql.Append("SELECT ").Append(string.Join(", ", selectColumns.Select(c => $"\"c\".{c}"))).Append(" FROM ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\"");
        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            sql.Append(" WHERE ").Append(whereSql);
        }
        sql.Append(" LIMIT 1");

        var results = await ExecuteReaderAsync(childMeta, sql.ToString(), parameters, conn, tx, ct).ConfigureAwait(false);
        if (results.Count == 0)
        {
            throw new RecordNotFoundException(childMeta.Name, relationName);
        }

        return ExtractPrimaryKeyValues(childMeta, results[0]);
    }

    private async Task<Dictionary<string, object?>> InsertChildReturningPkAsync(ModelMetadata childMeta, object createObj, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct, IReadOnlyDictionary<string, object?>? overrideScalars = null)
    {
        EnsureNoRelationWrites(childMeta, createObj);
        var pctx = new ParameterContext();
        var (columns, insertParams) = BuildInsertColumns(childMeta, createObj, pctx, overrideScalars: overrideScalars is null ? null : new Dictionary<string, object?>(overrideScalars, StringComparer.Ordinal));
        if (childMeta.PrimaryKey is null || childMeta.PrimaryKey.Fields.Count == 0)
        {
            throw new InvalidOperationException($"Model '{childMeta.Name}' does not have a primary key defined.");
        }

        var returning = string.Join(", ", childMeta.PrimaryKey.Fields.Select(QuoteIdentifier));

        var insertSql = new StringBuilder();
        insertSql.Append("INSERT INTO ").Append(QuoteIdentifier(childMeta.Name)).Append(' ');
        if (columns.Count == 0)
        {
            insertSql.Append("DEFAULT VALUES");
        }
        else
        {
            insertSql.Append('(')
                .Append(string.Join(", ", columns.Select(QuoteIdentifier)))
                .Append(") VALUES (")
                .Append(string.Join(", ", insertParams.Select(p => p.ParameterName)))
                .Append(')');
        }
        insertSql.Append(" RETURNING ").Append(returning);

        var results = await ExecuteReaderAsync(childMeta, insertSql.ToString(), insertParams.ToList(), conn, tx, ct).ConfigureAwait(false);
        if (results.Count == 0)
        {
            throw new VoidTouchException(childMeta.Name, "create");
        }

        return ExtractPrimaryKeyValues(childMeta, results[0]);
    }

    private async Task DeleteChildByPrimaryKeyAsync(ModelMetadata childMeta, ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> childPk, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct, string relationName)
    {
        var parameters = new List<NpgsqlParameter>();
        var pctx = new ParameterContext();
        var predicates = new List<string>();
        AppendChildPkPredicate(fk, childPk, "c", pctx, parameters, predicates);

        var sql = new StringBuilder();
        sql.Append("DELETE FROM ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\"");
        if (predicates.Count > 0)
        {
            sql.Append(" WHERE ").Append(string.Join(" AND ", predicates));
        }

        var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name).ConfigureAwait(false);
        if (affected == 0)
        {
            throw new RecordNotFoundException(childMeta.Name, relationName);
        }
    }

    private async Task<Dictionary<string, object?>> FetchSingleChildPkByForeignKeyAsync(ModelMetadata childMeta, ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct, string relationName)
    {
        var parameters = new List<NpgsqlParameter>();
        var pctx = new ParameterContext();
        var predicates = new List<string>();
        AppendFkPredicate(fk, parentPk, "c", pctx, parameters, predicates);

        if (predicates.Count == 0)
        {
            throw new InvalidOperationException($"Unable to build FK predicate for relation '{relationName}' on '{childMeta.Name}'.");
        }

        var selectColumns = BuildSelectColumns(childMeta, null, includePrimaryKey: true);
        var sql = new StringBuilder();
        sql.Append("SELECT ").Append(string.Join(", ", selectColumns.Select(c => $"\"c\".{c}")))
            .Append(" FROM ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" WHERE ")
            .Append(string.Join(" AND ", predicates))
            .Append(" LIMIT 1");

        var results = await ExecuteReaderAsync(childMeta, sql.ToString(), parameters, conn, tx, ct).ConfigureAwait(false);
        if (results.Count == 0)
        {
            throw new RecordNotFoundException(childMeta.Name, relationName);
        }

        return ExtractPrimaryKeyValues(childMeta, results[0]);
    }

    /// <summary>
    /// For Create, resolve parent-owned to-one relations up front so FK columns are populated in the initial insert.
    /// Returns the remaining relation payloads (child-owned or parent-owned not handled here) and FK override values.
    /// </summary>
    private async Task<(Dictionary<FieldMetadata, object> RemainingRelations, Dictionary<string, object?> ParentFkOverrides, Dictionary<FieldMetadata, Dictionary<string, object?>?> ParentOwnedPlaceholders)> ResolveParentOwnedFkOverridesForCreateAsync(
        ModelMetadata parentMeta,
        IReadOnlyDictionary<FieldMetadata, object> relationPayloads,
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        CancellationToken ct)
    {
        var remaining = new Dictionary<FieldMetadata, object>();
        var overrides = new Dictionary<string, object?>(StringComparer.Ordinal);
        var parentOwnedPlaceholders = new Dictionary<FieldMetadata, Dictionary<string, object?>?>();

        foreach (var kvp in relationPayloads)
        {
            var relationField = kvp.Key;
            var payload = kvp.Value;
            var childMeta = GetModelMetadata(relationField.ClrType);
            var resolution = ResolveRelationForeignKey(parentMeta, childMeta, relationField);

            if (resolution.Owner != ForeignKeyOwner.Parent || relationField.IsList)
            {
                remaining[relationField] = payload;
                continue;
            }

            var (fkOverrides, childPk) = await ResolveParentOwnedToOneForCreateAsync(parentMeta, childMeta, relationField, resolution.ForeignKey, payload, conn, tx, ct).ConfigureAwait(false);
            foreach (var ov in fkOverrides)
            {
                overrides[ov.Key] = ov.Value;
            }

            if (childPk is not null)
            {
                parentOwnedPlaceholders[relationField] = new Dictionary<string, object?>(childPk, StringComparer.Ordinal);
            }
        }

        return (remaining, overrides, parentOwnedPlaceholders);
    }

    /// <summary>
    /// Resolve a single parent-owned to-one relation for Create, producing FK override values.
    /// </summary>
    private async Task<(Dictionary<string, object?> Overrides, Dictionary<string, object?>? ChildPk)> ResolveParentOwnedToOneForCreateAsync(
        ModelMetadata parentMeta,
        ModelMetadata childMeta,
        FieldMetadata relationField,
        ForeignKeyMetadata fk,
        object payload,
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        CancellationToken ct)
    {
        var directives = new List<(string Name, object? Value)>
        {
            ("Create", GetProperty(payload, "Create")),
            ("ConnectOrCreate", GetProperty(payload, "ConnectOrCreate")),
            ("Connect", GetProperty(payload, "Connect")),
            ("Replace", GetProperty(payload, "Replace") ?? GetProperty(payload, "Set")),
            ("Upsert", GetProperty(payload, "Upsert")),
        };

        var active = directives.Where(d => d.Value is not null).ToList();
        if (active.Count > 1)
        {
            throw new NotSupportedException($"Only one nested write directive can be specified for relation '{relationField.Name}' on '{parentMeta.Name}'.");
        }

        if (active.Count == 0)
        {
            if (!FkAllowsNullOnLocal(parentMeta, fk))
            {
                throw new ForeignKeyViolationException(parentMeta.Name, "Create", fk.RelationName ?? relationField.Name, new InvalidOperationException($"Cannot create '{parentMeta.Name}' without linking required relation '{relationField.Name}'."));
            }
            return (new Dictionary<string, object?>(StringComparer.Ordinal), null);
        }

        var directive = active[0];

        Dictionary<string, object?> childPk;
        switch (directive.Name)
        {
            case "Connect":
                childPk = await FetchChildPkByWhereAsync(childMeta, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                break;
            case "ConnectOrCreate":
                {
                    var whereObj = GetRequiredProperty(directive.Value!, "Where");
                    var createObj = GetRequiredProperty(directive.Value!, "Create");
                    EnsureNoRelationWrites(childMeta, createObj);
                    EnsureWhereValuesPresentOnCreate(childMeta, whereObj, createObj, relationField.Name);

                    try
                    {
                        childPk = await FetchChildPkByWhereAsync(childMeta, whereObj, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    }
                    catch (RecordNotFoundException)
                    {
                        childPk = await InsertChildReturningPkAsync(childMeta, createObj, conn, tx, ct).ConfigureAwait(false);
                    }
                    break;
                }
            case "Create":
                childPk = await InsertChildReturningPkAsync(childMeta, directive.Value!, conn, tx, ct).ConfigureAwait(false);
                break;
            case "Replace":
                childPk = await FetchChildPkByWhereAsync(childMeta, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                break;
            case "Upsert":
                {
                    var createObj = GetRequiredProperty(directive.Value!, "Create");
                    var updateObj = GetRequiredProperty(directive.Value!, "Update");
                    EnsureNoRelationWrites(childMeta, createObj);
                    EnsureNoRelationWrites(childMeta, updateObj);
                    throw new NotSupportedException($"Upsert nested directive is not supported on Create for relation '{relationField.Name}'. Use ConnectOrCreate instead.");
                }
            default:
                throw new NotSupportedException($"Directive '{directive.Name}' is not supported for relation '{relationField.Name}' on '{parentMeta.Name}'.");
        }

        var principalFields = fk.PrincipalFields.Count > 0
            ? fk.PrincipalFields
            : childMeta.PrimaryKey?.Fields
              ?? throw new InvalidOperationException($"Foreign key '{fk.RelationName ?? relationField.Name}' for '{parentMeta.Name}' does not declare principal fields and '{childMeta.Name}' has no primary key.");

        if (fk.LocalFields.Count != principalFields.Count)
        {
            throw new InvalidOperationException($"Foreign key field count mismatch for relation '{relationField.Name}' on '{parentMeta.Name}': local {fk.LocalFields.Count} vs principal {principalFields.Count}.");
        }

        var mapped = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (int i = 0; i < fk.LocalFields.Count; i++)
        {
            var local = fk.LocalFields[i];
            var principal = principalFields[i];
            if (!childPk.TryGetValue(principal, out var val))
            {
                throw new InvalidOperationException($"Principal field '{principal}' not found on model '{childMeta.Name}' when resolving relation '{relationField.Name}'.");
            }
            mapped[local] = val;
        }

        return (mapped, childPk);
    }

    /// <summary>
    /// Executes nested writes for relations whose foreign keys live on the child (to-one and collections).
    /// Parent mutations supply PK values; this helper wires or unwires children by updating child FK columns.
    /// </summary>
    private async Task ExecuteRelationWritesAsync(ModelMetadata parentMeta, IReadOnlyDictionary<FieldMetadata, object> relationPayloads, object parentInstance, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        if (relationPayloads.Count == 0)
        {
            return;
        }

        var parentPk = ExtractPrimaryKeyValues(parentMeta, parentInstance);
        foreach (var kvp in relationPayloads)
        {
            var relationField = kvp.Key;
            var payload = kvp.Value;
            var childMeta = GetModelMetadata(relationField.ClrType);

            var resolution = ResolveRelationForeignKey(parentMeta, childMeta, relationField);
            var fk = resolution.ForeignKey;

            if (relationField.IsList)
            {
                await HandleChildOwnedCollectionWritesAsync(parentMeta, childMeta, relationField, fk, payload, parentPk, parentInstance, conn, tx, ct).ConfigureAwait(false);
                continue;
            }

            if (resolution.Owner == ForeignKeyOwner.Child)
            {
                await HandleChildOwnedToOneWritesAsync(parentMeta, childMeta, relationField, fk, payload, parentPk, parentInstance, conn, tx, ct).ConfigureAwait(false);
            }
            else
            {
                await HandleParentOwnedToOneWritesAsync(parentMeta, childMeta, relationField, fk, payload, parentPk, parentInstance, conn, tx, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Handles a single child-owned to-one relation directive (Create/Connect/Disconnect/Delete/Update/Upsert).
    /// One directive is allowed per payload to keep behavior deterministic.
    /// </summary>
    private async Task HandleChildOwnedToOneWritesAsync(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField, ForeignKeyMetadata fk, object payload, IReadOnlyDictionary<string, object?> parentPk, object parentInstance, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        var directives = new List<(string Name, object? Value)>
        {
            ("Create", GetProperty(payload, "Create")),
            ("ConnectOrCreate", GetProperty(payload, "ConnectOrCreate")),
            ("Connect", GetProperty(payload, "Connect")),
            ("Replace", GetProperty(payload, "Replace")), // removed Set alias
            ("Update", GetProperty(payload, "Update")),
            ("Upsert", GetProperty(payload, "Upsert")),
            ("DisconnectAll", GetBool(payload, "DisconnectAll") ? true : null),
            ("Disconnect", GetBool(payload, "Disconnect") ? true : null),
            ("Delete", GetBool(payload, "Delete") ? true : null)
        };

        var active = directives.Where(d => d.Value is not null).ToList();
        if (active.Count == 0)
        {
            return;
        }

        if (active.Count > 1)
        {
            throw new NotSupportedException($"Only one nested write directive can be specified for relation '{relationField.Name}' on '{parentMeta.Name}'.");
        }

        var directive = active[0];
        var paramCtx = new ParameterContext();

        switch (directive.Name)
        {
            case "Disconnect" when directive.Value is bool:
            case "DisconnectAll" when directive.Value is bool:
                if (!FkAllowsNull(fk, childMeta))
                {
                    throw new NotSupportedException($"Relation '{relationField.Name}' cannot disconnect because the child foreign key is non-nullable.");
                }
                await DisconnectChildrenByForeignKeyAsync(childMeta, fk, parentPk, conn, tx, ct).ConfigureAwait(false);
                ClearParentRelation(parentInstance, relationField);
                break;

            case "Delete" when directive.Value is bool:
                {
                    var parameters = new List<NpgsqlParameter>();
                    var predicates = new List<string>();
                    AppendFkPredicate(fk, parentPk, "c", paramCtx, parameters, predicates);
                    var sql = new StringBuilder();
                    sql.Append("DELETE FROM ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\"");
                    if (predicates.Count > 0)
                    {
                        sql.Append(" WHERE ").Append(string.Join(" AND ", predicates));
                    }
                    await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Delete.{relationField.Name}").ConfigureAwait(false);
                    ClearParentRelation(parentInstance, relationField);
                    break;
                }

            case "Create":
                {
                    EnsureNoRelationWrites(childMeta, directive.Value!);
                    var overrides = BuildFkOverrideValues(fk, parentPk);
                    var childPk = await InsertChildReturningPkAsync(childMeta, directive.Value!, conn, tx, ct, overrides).ConfigureAwait(false);
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "Connect":
                {
                    var childPk = await FetchChildPkByWhereAsync(childMeta, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    await ConnectChildByWhereAsync(childMeta, fk, parentPk, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "ConnectOrCreate":
                {
                    var whereObj = GetRequiredProperty(directive.Value!, "Where");
                    var createObj = GetRequiredProperty(directive.Value!, "Create");
                    EnsureNoRelationWrites(childMeta, createObj);
                    EnsureWhereValuesPresentOnCreate(childMeta, whereObj, createObj, relationField.Name);

                    try
                    {
                        var childPk = await FetchChildPkByWhereAsync(childMeta, whereObj, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                        await ConnectChildByWhereAsync(childMeta, fk, parentPk, whereObj, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                        AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                        break;
                    }
                    catch (RecordNotFoundException)
                    {
                        // fall through to create
                    }

                    var overrides = BuildFkOverrideValues(fk, parentPk);
                    var childPkCreated = await InsertChildReturningPkAsync(childMeta, createObj, conn, tx, ct, overrides).ConfigureAwait(false);
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPkCreated);
                    break;
                }

            case "Replace":
                {
                    if (!FkAllowsNull(fk, childMeta))
                    {
                        throw new NotSupportedException($"Relation '{relationField.Name}' cannot replace because the child foreign key is non-nullable.");
                    }

                    await DisconnectChildrenByForeignKeyAsync(childMeta, fk, parentPk, conn, tx, ct).ConfigureAwait(false);
                    ClearParentRelation(parentInstance, relationField);
                    var childPk = await FetchChildPkByWhereAsync(childMeta, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    await ConnectChildByWhereAsync(childMeta, fk, parentPk, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "Update":
                {
                    EnsureNoRelationWrites(childMeta, directive.Value!);
                    var childPk = await FetchSingleChildPkByForeignKeyAsync(childMeta, fk, parentPk, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    var parameters = new List<NpgsqlParameter>();
                    var (setSql, setParams, _) = BuildUpdateSet(childMeta, directive.Value!, paramCtx);
                    parameters.AddRange(setParams);
                    var whereParts = new List<string>();
                    AppendFkPredicate(fk, parentPk, "c", paramCtx, parameters, whereParts);

                    var sql = new StringBuilder();
                    sql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ").Append(setSql);
                    if (whereParts.Count > 0)
                    {
                        sql.Append(" WHERE ").Append(string.Join(" AND ", whereParts));
                    }

                    var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Update.{relationField.Name}").ConfigureAwait(false);
                    if (affected == 0)
                    {
                        throw new RecordNotFoundException(childMeta.Name, $"Update.{relationField.Name}");
                    }
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "Upsert":
                {
                    var updateObj = GetRequiredProperty(directive.Value!, "Update");
                    var createObj = GetRequiredProperty(directive.Value!, "Create");

                    EnsureNoRelationWrites(childMeta, updateObj);
                    EnsureNoRelationWrites(childMeta, createObj);

                    Dictionary<string, object?> childPk;
                    try
                    {
                        childPk = await FetchSingleChildPkByForeignKeyAsync(childMeta, fk, parentPk, conn, tx, ct, relationField.Name).ConfigureAwait(false);

                        var parameters = new List<NpgsqlParameter>();
                        var whereParts = new List<string>();
                        AppendFkPredicate(fk, parentPk, "c", paramCtx, parameters, whereParts);

                        var (setSql, setParams, _) = BuildUpdateSet(childMeta, updateObj, paramCtx);
                        parameters.AddRange(setParams);

                        var updateSql = new StringBuilder();
                        updateSql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ").Append(setSql);
                        if (whereParts.Count > 0)
                        {
                            updateSql.Append(" WHERE ").Append(string.Join(" AND ", whereParts));
                        }

                        var affected = await ExecuteNonQueryInternalAsync(updateSql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Upsert.{relationField.Name}").ConfigureAwait(false);
                        if (affected > 0)
                        {
                            AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                            break;
                        }
                    }
                    catch (RecordNotFoundException)
                    {
                        childPk = new Dictionary<string, object?>();
                    }

                    var overrides = BuildFkOverrideValues(fk, parentPk);
                    var newChildPk = await InsertChildReturningPkAsync(childMeta, createObj, conn, tx, ct, overrides).ConfigureAwait(false);
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, newChildPk);
                    break;
                }

            default:
                throw new NotSupportedException($"Directive '{directive.Name}' is not supported for relation '{relationField.Name}' on '{parentMeta.Name}'.");
        }
    }

    /// <summary>
    /// Handles child-owned collection directives (Create/Connect/Disconnect/Delete/Update/Upsert variants).
    /// These operations gate on FK nullability for disconnect-style directives.
    /// </summary>
    private async Task HandleChildOwnedCollectionWritesAsync(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField, ForeignKeyMetadata fk, object payload, IReadOnlyDictionary<string, object?> parentPk, object parentInstance, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        var directives = new List<object?>
        {
            GetBool(payload, "DisconnectAll") ? true : null,
            GetEnumerable(payload, "Replace"), // removed Set alias
            GetEnumerable(payload, "Delete"),
            GetEnumerable(payload, "Disconnect"),
            GetEnumerable(payload, "Connect"),
            GetEnumerable(payload, "Create"),
            GetEnumerable(payload, "Update"),
            GetEnumerable(payload, "UpdateMany"),
            GetEnumerable(payload, "ConnectOrCreate"),
            GetEnumerable(payload, "Upsert")
        };

        var activeCount = directives.Count(d => d is not null);
        if (activeCount > 1)
        {
            throw new NotSupportedException($"Only one nested write directive can be specified for relation '{relationField.Name}' on '{parentMeta.Name}'.");
        }

        if (GetBool(payload, "DisconnectAll"))
        {
            if (!FkAllowsNull(fk, childMeta))
            {
                throw new NotSupportedException($"Relation '{relationField.Name}' cannot disconnect because the child foreign key is non-nullable.");
            }
            await DisconnectChildrenByForeignKeyAsync(childMeta, fk, parentPk, conn, tx, ct).ConfigureAwait(false);
        }

        var replaceSet = GetEnumerable(payload, "Replace") ?? GetEnumerable(payload, "Set");
        if (replaceSet is not null)
        {
            if (!FkAllowsNull(fk, childMeta))
            {
                throw new NotSupportedException($"Relation '{relationField.Name}' cannot replace because the child foreign key is non-nullable.");
            }

            await DisconnectChildrenByForeignKeyAsync(childMeta, fk, parentPk, conn, tx, ct).ConfigureAwait(false);
            ClearParentRelation(parentInstance, relationField);
            foreach (var whereObj in replaceSet)
            {
                var childPk = await FetchChildPkByWhereAsync(childMeta, whereObj!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                await ConnectChildByWhereAsync(childMeta, fk, parentPk, whereObj!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
            }
        }

        var deleteList = GetEnumerable(payload, "Delete");
        if (deleteList is not null)
        {
            foreach (var whereObj in deleteList)
            {
                var parameters = new List<NpgsqlParameter>();
                var pctx = new ParameterContext();
                var whereSql = BuildWhereUnique(childMeta, whereObj!, parameters, pctx, "c");
                var fkPredicates = new List<string>();
                AppendFkPredicate(fk, parentPk, "c", pctx, parameters, fkPredicates);
                var clauses = new List<string>();
                if (!string.IsNullOrWhiteSpace(whereSql)) clauses.Add(whereSql);
                if (fkPredicates.Count > 0) clauses.Add(string.Join(" AND ", fkPredicates));

                var sql = new StringBuilder();
                sql.Append("DELETE FROM ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\"");
                if (clauses.Count > 0)
                {
                    sql.Append(" WHERE ").Append(string.Join(" AND ", clauses));
                }

                await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Delete.{relationField.Name}").ConfigureAwait(false);
            }
        }

        var deleteManyList = GetEnumerable(payload, "DeleteMany");
        if (deleteManyList is not null)
        {
            foreach (var filter in deleteManyList)
            {
                var parameters = new List<NpgsqlParameter>();
                var pctx = new ParameterContext();
                var aliasCtx = new AliasContext();
                var whereSql = BuildWhere(childMeta, filter!, parameters, pctx, "c", ref aliasCtx);
                var fkPredicates = new List<string>();
                AppendFkPredicate(fk, parentPk, "c", pctx, parameters, fkPredicates);

                var clauses = new List<string>();
                if (!string.IsNullOrWhiteSpace(whereSql)) clauses.Add(whereSql);
                if (fkPredicates.Count > 0) clauses.Add(string.Join(" AND ", fkPredicates));

                var sql = new StringBuilder();
                sql.Append("DELETE FROM ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\"");
                if (clauses.Count > 0)
                {
                    sql.Append(" WHERE ").Append(string.Join(" AND ", clauses));
                }

                await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"DeleteMany.{relationField.Name}").ConfigureAwait(false);
            }
        }

        var disconnectList = GetEnumerable(payload, "Disconnect");
        if (disconnectList is not null)
        {
            if (!FkAllowsNull(fk, childMeta))
            {
                throw new NotSupportedException($"Relation '{relationField.Name}' cannot disconnect because the child foreign key is non-nullable.");
            }

            foreach (var whereObj in disconnectList)
            {
                var parameters = new List<NpgsqlParameter>();
                var pctx = new ParameterContext();
                var whereSql = BuildWhereUnique(childMeta, whereObj!, parameters, pctx, "c");
                var assignments = new List<string>();
                AppendFkAssignments(fk, parentPk, pctx, parameters, assignments, setNull: true);

                var sql = new StringBuilder();
                sql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ")
                    .Append(string.Join(", ", assignments));
                if (!string.IsNullOrWhiteSpace(whereSql))
                {
                    sql.Append(" WHERE ").Append(whereSql);
                }

                await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Disconnect.{relationField.Name}").ConfigureAwait(false);
            }
        }

        var updateList = GetEnumerable(payload, "Update");
        if (updateList is not null)
        {
            foreach (var updateObj in updateList)
            {
                var where = GetRequiredProperty(updateObj!, "Where");
                var data = GetRequiredProperty(updateObj!, "Data");
                EnsureNoRelationWrites(childMeta, data);

                var childPk = await FetchChildPkByWhereAsync(childMeta, where!, conn, tx, ct, relationField.Name).ConfigureAwait(false);

                var parameters = new List<NpgsqlParameter>();
                var pctx = new ParameterContext();
                var (setSql, setParams, _) = BuildUpdateSet(childMeta, data, pctx);
                parameters.AddRange(setParams);
                var whereSql = BuildWhereUnique(childMeta, where, parameters, pctx, "c");
                var fkPred = new List<string>();
                AppendFkPredicate(fk, parentPk, "c", pctx, parameters, fkPred);

                var clauses = new List<string>();
                if (!string.IsNullOrWhiteSpace(whereSql)) clauses.Add(whereSql);
                if (fkPred.Count > 0) clauses.Add(string.Join(" AND ", fkPred));

                var sql = new StringBuilder();
                sql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ").Append(setSql);
                if (clauses.Count > 0)
                {
                    sql.Append(" WHERE ").Append(string.Join(" AND ", clauses));
                }

                var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Update.{relationField.Name}").ConfigureAwait(false);
                if (affected > 0)
                {
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                }
            }
        }

        var updateManyList = GetEnumerable(payload, "UpdateMany");
        if (updateManyList is not null)
        {
            foreach (var updateMany in updateManyList)
            {
                var data = GetRequiredProperty(updateMany!, "Data");
                var where = GetRequiredProperty(updateMany!, "Where");
                EnsureNoRelationWrites(childMeta, data);

                var parameters = new List<NpgsqlParameter>();
                var pctx = new ParameterContext();
                var (setSql, setParams, _) = BuildUpdateSet(childMeta, data, pctx);
                parameters.AddRange(setParams);
                var aliasCtx = new AliasContext();
                var whereSql = BuildWhere(childMeta, where, parameters, pctx, "c", ref aliasCtx);
                var fkPred = new List<string>();
                AppendFkPredicate(fk, parentPk, "c", pctx, parameters, fkPred);

                var clauses = new List<string>();
                if (!string.IsNullOrWhiteSpace(whereSql)) clauses.Add(whereSql);
                if (fkPred.Count > 0) clauses.Add(string.Join(" AND ", fkPred));

                var sql = new StringBuilder();
                sql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ").Append(setSql);
                if (clauses.Count > 0)
                {
                    sql.Append(" WHERE ").Append(string.Join(" AND ", clauses));
                }

                await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"UpdateMany.{relationField.Name}").ConfigureAwait(false);
            }
        }

        var connectOrCreateList = GetEnumerable(payload, "ConnectOrCreate");
        if (connectOrCreateList is not null)
        {
            foreach (var item in connectOrCreateList)
            {
                var whereObj = GetRequiredProperty(item!, "Where");
                var createObj = GetRequiredProperty(item!, "Create");
                EnsureNoRelationWrites(childMeta, createObj);
                EnsureWhereValuesPresentOnCreate(childMeta, whereObj, createObj, relationField.Name);

                try
                {
                    var childPk = await FetchChildPkByWhereAsync(childMeta, whereObj, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    await ConnectChildByWhereAsync(childMeta, fk, parentPk, whereObj, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
                    continue;
                }
                catch (RecordNotFoundException)
                {
                    // fall through to create
                }

                var overrides = BuildFkOverrideValues(fk, parentPk);
                var childPkCreated = await InsertChildReturningPkAsync(childMeta, createObj, conn, tx, ct, overrides).ConfigureAwait(false);
                AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPkCreated);
            }
        }

        var connectList = GetEnumerable(payload, "Connect");
        if (connectList is not null)
        {
            foreach (var whereObj in connectList)
            {
                var childPk = await FetchChildPkByWhereAsync(childMeta, whereObj!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                await ConnectChildByWhereAsync(childMeta, fk, parentPk, whereObj!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
            }
        }

        var createList = GetEnumerable(payload, "Create");
        if (createList is not null)
        {
            foreach (var createObj in createList)
            {
                EnsureNoRelationWrites(childMeta, createObj!);
                var overrides = BuildFkOverrideValues(fk, parentPk);
                var childPk = await InsertChildReturningPkAsync(childMeta, createObj!, conn, tx, ct, overrides).ConfigureAwait(false);
                AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, childPk);
            }
        }

        var upsertList = GetEnumerable(payload, "Upsert");
        if (upsertList is not null)
        {
            foreach (var item in upsertList)
            {
                var createObj = GetRequiredProperty(item!, "Create");
                var updateObj = GetRequiredProperty(item!, "Update");
                EnsureNoRelationWrites(childMeta, createObj);
                EnsureNoRelationWrites(childMeta, updateObj);

                var parameters = new List<NpgsqlParameter>();
                var pctx = new ParameterContext();
                var fkPred = new List<string>();
                AppendFkPredicate(fk, parentPk, "c", pctx, parameters, fkPred);
                var (setSql, setParams, _) = BuildUpdateSet(childMeta, updateObj, pctx);
                parameters.AddRange(setParams);

                var updateSql = new StringBuilder();
                updateSql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ").Append(setSql);
                if (fkPred.Count > 0)
                {
                    updateSql.Append(" WHERE ").Append(string.Join(" AND ", fkPred));
                }

                var affected = await ExecuteNonQueryInternalAsync(updateSql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Upsert.{relationField.Name}").ConfigureAwait(false);
                if (affected > 0)
                {
                    continue;
                }

                var overrides = BuildFkOverrideValues(fk, parentPk);
                var insertedPk = await InsertChildReturningPkAsync(childMeta, createObj, conn, tx, ct, overrides).ConfigureAwait(false);
                AddChildPlaceholderToCollection(childMeta, relationField, parentInstance, insertedPk);
            }
        }
    }

    /// <summary>
    /// Handles parent-owned to-one nested writes by updating the parent's FK columns instead of the child.
    /// </summary>
    private async Task HandleParentOwnedToOneWritesAsync(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField, ForeignKeyMetadata fk, object payload, IReadOnlyDictionary<string, object?> parentPk, object parentInstance, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        var directives = new List<(string Name, object? Value)>
        {
            ("Create", GetProperty(payload, "Create")),
            ("ConnectOrCreate", GetProperty(payload, "ConnectOrCreate")),
            ("Connect", GetProperty(payload, "Connect")),
            ("Replace", GetProperty(payload, "Replace")), // removed Set alias
            ("Update", GetProperty(payload, "Update")),
            ("Upsert", GetProperty(payload, "Upsert")),
            ("DisconnectAll", GetBool(payload, "DisconnectAll") ? true : null),
            ("Disconnect", GetBool(payload, "Disconnect") ? true : null),
            ("Delete", GetBool(payload, "Delete") ? true : null)
        };

        var active = directives.Where(d => d.Value is not null).ToList();
        if (active.Count == 0)
        {
            return;
        }

        if (active.Count > 1)
        {
            throw new NotSupportedException($"Only one nested write directive can be specified for relation '{relationField.Name}' on '{parentMeta.Name}'.");
        }

        var directive = active[0];

        switch (directive.Name)
        {
            case "Disconnect" when directive.Value is bool:
            case "DisconnectAll" when directive.Value is bool:
                if (!FkAllowsNullOnLocal(parentMeta, fk))
                {
                    throw new NotSupportedException($"Relation '{relationField.Name}' cannot disconnect because the parent foreign key is non-nullable.");
                }
                await ClearParentForeignKeyAsync(parentMeta, fk, parentPk, conn, tx, ct).ConfigureAwait(false);
                ApplyParentForeignKeyLocally(parentMeta, fk, childMeta, parentInstance, new Dictionary<string, object?>(StringComparer.Ordinal), setNull: true);
                ClearParentRelation(parentInstance, relationField);
                break;

            case "Delete" when directive.Value is bool:
                {
                    var (childPk, hasNull) = ExtractForeignKeyValuesFromParent(parentMeta, fk, parentInstance);
                    if (hasNull)
                    {
                        throw new RecordNotFoundException(childMeta.Name, $"Delete.{relationField.Name}");
                    }
                    if (!FkAllowsNullOnLocal(parentMeta, fk))
                    {
                        throw new NotSupportedException($"Relation '{relationField.Name}' cannot delete because the parent foreign key is non-nullable.");
                    }

                    await ClearParentForeignKeyAsync(parentMeta, fk, parentPk, conn, tx, ct).ConfigureAwait(false);
                    ApplyParentForeignKeyLocally(parentMeta, fk, childMeta, parentInstance, new Dictionary<string, object?>(StringComparer.Ordinal), setNull: true);
                    ClearParentRelation(parentInstance, relationField);
                    await DeleteChildByPrimaryKeyAsync(childMeta, fk, childPk, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    break;
                }

            case "Create":
                {
                    EnsureNoRelationWrites(childMeta, directive.Value!);
                    var childPk = await InsertChildReturningPkAsync(childMeta, directive.Value!, conn, tx, ct).ConfigureAwait(false);
                    await SetParentForeignKeyAsync(parentMeta, fk, parentPk, childPk, conn, tx, ct).ConfigureAwait(false);
                    ApplyParentForeignKeyLocally(parentMeta, fk, childMeta, parentInstance, childPk);
                    ApplyParentRelationPlaceholder(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "Connect":
                {
                    var childPk = await FetchChildPkByWhereAsync(childMeta, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    await SetParentForeignKeyAsync(parentMeta, fk, parentPk, childPk, conn, tx, ct).ConfigureAwait(false);
                    ApplyParentForeignKeyLocally(parentMeta, fk, childMeta, parentInstance, childPk);
                    ApplyParentRelationPlaceholder(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "ConnectOrCreate":
                {
                    var whereObj = GetRequiredProperty(directive.Value!, "Where");
                    var createObj = GetRequiredProperty(directive.Value!, "Create");
                    EnsureNoRelationWrites(childMeta, createObj);
                    EnsureWhereValuesPresentOnCreate(childMeta, whereObj, createObj, relationField.Name);

                    Dictionary<string, object?> childPk;
                    try
                    {
                        childPk = await FetchChildPkByWhereAsync(childMeta, whereObj, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    }
                    catch (RecordNotFoundException)
                    {
                        childPk = await InsertChildReturningPkAsync(childMeta, createObj, conn, tx, ct).ConfigureAwait(false);
                    }

                    await SetParentForeignKeyAsync(parentMeta, fk, parentPk, childPk, conn, tx, ct).ConfigureAwait(false);
                    ApplyParentForeignKeyLocally(parentMeta, fk, childMeta, parentInstance, childPk);
                    ApplyParentRelationPlaceholder(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "Replace":
                {
                    var childPk = await FetchChildPkByWhereAsync(childMeta, directive.Value!, conn, tx, ct, relationField.Name).ConfigureAwait(false);
                    await SetParentForeignKeyAsync(parentMeta, fk, parentPk, childPk, conn, tx, ct).ConfigureAwait(false);
                    ApplyParentForeignKeyLocally(parentMeta, fk, childMeta, parentInstance, childPk);
                    ApplyParentRelationPlaceholder(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "Update":
                {
                    var (childPk, hasNull) = ExtractForeignKeyValuesFromParent(parentMeta, fk, parentInstance);
                    if (hasNull)
                    {
                        throw new RecordNotFoundException(childMeta.Name, $"Update.{relationField.Name}");
                    }

                    EnsureNoRelationWrites(childMeta, directive.Value!);
                    var parameters = new List<NpgsqlParameter>();
                    var pctx = new ParameterContext();
                    var (setSql, setParams, _) = BuildUpdateSet(childMeta, directive.Value!, pctx);
                    parameters.AddRange(setParams);
                    var predicates = new List<string>();
                    AppendChildPkPredicate(fk, childPk, "c", pctx, parameters, predicates);

                    var sql = new StringBuilder();
                    sql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ").Append(setSql);
                    if (predicates.Count > 0)
                    {
                        sql.Append(" WHERE ").Append(string.Join(" AND ", predicates));
                    }

                    var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Update.{relationField.Name}").ConfigureAwait(false);
                    if (affected == 0)
                    {
                        throw new RecordNotFoundException(childMeta.Name, $"Update.{relationField.Name}");
                    }
                    ApplyParentRelationPlaceholder(childMeta, relationField, parentInstance, childPk);
                    break;
                }

            case "Upsert":
                {
                    var updateObj = GetRequiredProperty(directive.Value!, "Update");
                    var createObj = GetRequiredProperty(directive.Value!, "Create");
                    EnsureNoRelationWrites(childMeta, updateObj);
                    EnsureNoRelationWrites(childMeta, createObj);

                    var (childPk, hasNull) = ExtractForeignKeyValuesFromParent(parentMeta, fk, parentInstance);
                    if (!hasNull)
                    {
                        var parameters = new List<NpgsqlParameter>();
                        var pctx = new ParameterContext();
                        var (setSql, setParams, _) = BuildUpdateSet(childMeta, updateObj, pctx);
                        parameters.AddRange(setParams);
                        var predicates = new List<string>();
                        AppendChildPkPredicate(fk, childPk, "c", pctx, parameters, predicates);

                        var updateSql = new StringBuilder();
                        updateSql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ").Append(setSql);
                        if (predicates.Count > 0)
                        {
                            updateSql.Append(" WHERE ").Append(string.Join(" AND ", predicates));
                        }

                        var affected = await ExecuteNonQueryInternalAsync(updateSql.ToString(), parameters, conn, tx, ct, childMeta.Name, $"Upsert.{relationField.Name}").ConfigureAwait(false);
                        if (affected > 0)
                        {
                            break;
                        }
                    }

                    var newChildPk = await InsertChildReturningPkAsync(childMeta, createObj, conn, tx, ct).ConfigureAwait(false);
                    await SetParentForeignKeyAsync(parentMeta, fk, parentPk, newChildPk, conn, tx, ct).ConfigureAwait(false);
                    ApplyParentForeignKeyLocally(parentMeta, fk, childMeta, parentInstance, newChildPk);
                    ApplyParentRelationPlaceholder(childMeta, relationField, parentInstance, newChildPk);
                    break;
                }

            default:
                throw new NotSupportedException($"Directive '{directive.Name}' is not supported for relation '{relationField.Name}' on '{parentMeta.Name}'.");
        }
    }

    private static Dictionary<string, object?> BuildFkOverrideValues(ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk)
    {
        var overrides = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (int i = 0; i < fk.LocalFields.Count; i++)
        {
            var principalField = fk.PrincipalFields[i];
            if (!parentPk.TryGetValue(principalField, out var value))
            {
                throw new InvalidOperationException($"Parent primary key field '{principalField}' was not found while constructing FK overrides.");
            }

            overrides[fk.LocalFields[i]] = value;
        }

        return overrides;
    }

    private async Task DisconnectChildrenByForeignKeyAsync(ModelMetadata childMeta, ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct)
    {
        var parameters = new List<NpgsqlParameter>();
        var paramCtx = new ParameterContext();
        var assignments = new List<string>();
        AppendFkAssignments(fk, parentPk, paramCtx, parameters, assignments, setNull: true);

        var predicates = new List<string>();
        AppendFkPredicate(fk, parentPk, "c", paramCtx, parameters, predicates);

        var sql = new StringBuilder();
        sql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ")
            .Append(string.Join(", ", assignments));
        if (predicates.Count > 0)
        {
            sql.Append(" WHERE ").Append(string.Join(" AND ", predicates));
        }

        await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name).ConfigureAwait(false);
    }

    private async Task ConnectChildByWhereAsync(ModelMetadata childMeta, ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk, object whereObj, NpgsqlConnection conn, NpgsqlTransaction tx, CancellationToken ct, string relationName)
    {
        var parameters = new List<NpgsqlParameter>();
        var paramCtx = new ParameterContext();
        var whereSql = BuildWhereUnique(childMeta, whereObj, parameters, paramCtx, "c");
        var assignments = new List<string>();
        AppendFkAssignments(fk, parentPk, paramCtx, parameters, assignments);

        var sql = new StringBuilder();
        sql.Append("UPDATE ").Append(QuoteIdentifier(childMeta.Name)).Append(" AS \"c\" SET ")
            .Append(string.Join(", ", assignments));
        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            sql.Append(" WHERE ").Append(whereSql);
        }

        var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, conn, tx, ct, childMeta.Name).ConfigureAwait(false);
        if (affected == 0)
        {
            throw new RecordNotFoundException(childMeta.Name, $"Connect.{fk.RelationName ?? relationName}");
        }
    }

    private static bool GetBool(object payload, string propName)
    {
        return payload.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance)?.GetValue(payload) as bool? == true;
    }

    private static System.Collections.IEnumerable? GetEnumerable(object payload, string propName)
    {
        return payload.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance)?.GetValue(payload) as System.Collections.IEnumerable;
    }
}
