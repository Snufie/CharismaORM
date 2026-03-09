using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Charisma.Schema;
using System.Threading;
using System.Threading.Tasks;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Charisma.QueryEngine.Planning;
using Npgsql;
using NpgsqlTypes;
using PlanningIncludePlan = Charisma.QueryEngine.Planning.IncludePlan;

namespace Charisma.QueryEngine.Execution;

public sealed partial class PostgresSqlExecutor
{
    private async Task<List<object>> ExecuteReaderAsync(ModelMetadata meta, string sql, List<NpgsqlParameter> parameters, CancellationToken ct, string operation = "Execute")
    {
        await using var dbConn = await _connectionProvider.OpenAsync(ct).ConfigureAwait(false);
        if (dbConn is not NpgsqlConnection conn)
        {
            throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
        }

        return await ExecuteReaderInternalAsync(meta, sql, parameters, conn, null, ct, operation).ConfigureAwait(false);
    }

    private async Task<List<object>> ExecuteReaderAsync(ModelMetadata meta, string sql, List<NpgsqlParameter> parameters, NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct, string operation = "Execute")
    {
        return await ExecuteReaderInternalAsync(meta, sql, parameters, conn, tx, ct, operation).ConfigureAwait(false);
    }

    private async Task<List<object>> ExecuteReaderInternalAsync(ModelMetadata meta, string sql, List<NpgsqlParameter> parameters, NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct, string operation)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn)
            {
                Transaction = tx
            };
            foreach (var p in parameters)
            {
                cmd.Parameters.Add(p);
            }

            var results = new List<object>();
            var modelType = _modelTypeResolver(meta.Name);
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var instance = Activator.CreateInstance(modelType) ?? throw new InvalidOperationException($"Failed to create instance of '{modelType.FullName}'.");
                MaterializeRow(meta, reader, instance);
                results.Add(instance);
            }

            var logEnv = Environment.GetEnvironmentVariable("CHARISMA_LOG_SQL");
            var logSql = string.Equals(logEnv?.Trim(), "1", StringComparison.Ordinal)
                || string.Equals(logEnv?.Trim(), "true", StringComparison.OrdinalIgnoreCase);
            if (logSql)
            {
                Console.WriteLine($"[SQL rows {operation}] {results.Count}");
            }

            return results;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new UniqueConstraintViolationException(meta.Name, operation, ex.ConstraintName, ex);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
        {
            throw new ForeignKeyViolationException(meta.Name, operation, ex.ConstraintName, ex);
        }
    }

    private async Task<List<object>> ExecuteReaderWithIncludesAsync(ModelMetadata meta, IReadOnlyList<PlanningIncludePlan> includes, string sql, List<NpgsqlParameter> parameters, CancellationToken ct, string operation = "Execute", NpgsqlConnection? connOverride = null, NpgsqlTransaction? txOverride = null)
    {
        try
        {
            var ownsConnection = connOverride is null;
            await using var ownedConn = ownsConnection
                ? await _connectionProvider.OpenAsync(ct).ConfigureAwait(false)
                : null;

            var conn = connOverride ?? ownedConn as NpgsqlConnection;
            if (conn is null)
            {
                throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
            }

            await using var cmd = new NpgsqlCommand(sql, conn)
            {
                Transaction = txOverride
            };
            foreach (var p in parameters)
            {
                cmd.Parameters.Add(p);
            }

            var rootType = _modelTypeResolver(meta.Name);
            var roots = new Dictionary<string, object>(StringComparer.Ordinal);
            var collectionCache = new Dictionary<string, Dictionary<string, object>>(StringComparer.Ordinal);
            var hasIncludes = includes.Count > 0;

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!hasIncludes)
            {
                var results = new List<object>();
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    var instance = Activator.CreateInstance(rootType) ?? throw new InvalidOperationException($"Failed to create instance of '{rootType.FullName}'.");
                    MaterializeInto(meta, reader, instance, prefix: string.Empty);
                    results.Add(instance);
                }

                var logEnvSimple = Environment.GetEnvironmentVariable("CHARISMA_LOG_SQL");
                var logSqlSimple = string.Equals(logEnvSimple?.Trim(), "1", StringComparison.Ordinal)
                    || string.Equals(logEnvSimple?.Trim(), "true", StringComparison.OrdinalIgnoreCase);
                if (logSqlSimple)
                {
                    Console.WriteLine($"[SQL rows {operation}] {results.Count}");
                }

                return results;
            }

            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var rootKey = BuildKey(meta, reader, prefix: string.Empty);
                if (rootKey is null)
                {
                    continue;
                }

                if (!roots.TryGetValue(rootKey, out var rootInstance))
                {
                    rootInstance = Activator.CreateInstance(rootType) ?? throw new InvalidOperationException($"Failed to create instance of '{rootType.FullName}'.");
                    MaterializeInto(meta, reader, rootInstance, prefix: string.Empty);
                    roots[rootKey] = rootInstance;
                }

                foreach (var include in includes)
                {
                    HydrateInclude(include, reader, rootInstance, rootKey, collectionCache);
                }
            }

            var logEnv = Environment.GetEnvironmentVariable("CHARISMA_LOG_SQL");
            var logSql = string.Equals(logEnv?.Trim(), "1", StringComparison.Ordinal)
                || string.Equals(logEnv?.Trim(), "true", StringComparison.OrdinalIgnoreCase);
            if (logSql)
            {
                Console.WriteLine($"[SQL rows {operation}] {roots.Count}");
            }

            return [.. roots.Values];
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new UniqueConstraintViolationException(meta.Name, operation, ex.ConstraintName, ex);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
        {
            throw new ForeignKeyViolationException(meta.Name, operation, ex.ConstraintName, ex);
        }
    }

    private static string? BuildKey(ModelMetadata meta, IDataRecord reader, string prefix)
    {
        if (meta.PrimaryKey is null)
        {
            throw new InvalidOperationException($"Model '{meta.Name}' lacks primary key metadata required for include materialization.");
        }

        var parts = new List<string>();
        foreach (var pkField in meta.PrimaryKey.Fields)
        {
            var columnName = string.IsNullOrEmpty(prefix) ? pkField : $"{prefix}{pkField}";
            var ordinal = TryGetOrdinal(reader, columnName);
            if (ordinal < 0 || reader.IsDBNull(ordinal))
            {
                return null;
            }
            parts.Add(reader.GetValue(ordinal)?.ToString() ?? string.Empty);
        }

        return string.Join("|", parts);
    }

    private void HydrateInclude(PlanningIncludePlan plan, IDataRecord reader, object parentInstance, string parentKey, Dictionary<string, Dictionary<string, object>> collectionCache)
    {
        if (plan.IsCollection)
        {
            var childKey = BuildKey(plan.Meta, reader, plan.ColumnPrefix);
            if (childKey is null)
            {
                return;
            }

            var cacheKey = $"{parentKey}|{plan.RelationName}";
            if (!collectionCache.TryGetValue(cacheKey, out var childMap))
            {
                childMap = new Dictionary<string, object>(StringComparer.Ordinal);
                collectionCache[cacheKey] = childMap;
            }

            if (!childMap.TryGetValue(childKey, out var childInstance))
            {
                var modelType = _modelTypeResolver(plan.Meta.Name);
                childInstance = Activator.CreateInstance(modelType) ?? throw new InvalidOperationException($"Failed to create instance of '{modelType.FullName}'.");
                MaterializeInto(plan.Meta, reader, childInstance, plan.ColumnPrefix);
                childMap[childKey] = childInstance;
                AttachToCollection(parentInstance, plan.RelationName, childInstance);
            }

            foreach (var child in plan.Children)
            {
                HydrateInclude(child, reader, childInstance, childKey, collectionCache);
            }
        }
        else
        {
            var childKey = BuildKey(plan.Meta, reader, plan.ColumnPrefix);
            if (childKey is null)
            {
                SetReference(parentInstance, plan.RelationName, null);
                return;
            }

            var prop = parentInstance.GetType().GetProperty(plan.RelationName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            var childInstance = prop?.GetValue(parentInstance);
            if (childInstance is null)
            {
                var modelType = _modelTypeResolver(plan.Meta.Name);
                childInstance = Activator.CreateInstance(modelType) ?? throw new InvalidOperationException($"Failed to create instance of '{modelType.FullName}'.");
                MaterializeInto(plan.Meta, reader, childInstance, plan.ColumnPrefix);
                SetReference(parentInstance, plan.RelationName, childInstance);
            }

            foreach (var child in plan.Children)
            {
                HydrateInclude(child, reader, childInstance, childKey, collectionCache);
            }
        }
    }

    private static void SetReference(object parentInstance, string relationName, object? childInstance)
    {
        var prop = parentInstance.GetType().GetProperty(relationName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (prop?.CanWrite == true)
        {
            prop.SetValue(parentInstance, childInstance);
        }
    }

    private static void AttachToCollection(object parentInstance, string relationName, object childInstance)
    {
        var prop = parentInstance.GetType().GetProperty(relationName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (prop is null || !prop.CanWrite)
        {
            return;
        }

        var currentValue = prop.GetValue(parentInstance);
        if (currentValue is System.Collections.IList list)
        {
            list.Add(childInstance);
            return;
        }

        var genericArgs = prop.PropertyType.GetGenericArguments();
        var elementType = genericArgs.FirstOrDefault() ?? childInstance.GetType();
        var listType = typeof(List<>).MakeGenericType(elementType);
        var newList = Activator.CreateInstance(listType) as System.Collections.IList;
        newList?.Add(childInstance);
        prop.SetValue(parentInstance, newList);
    }

    private static int TryGetOrdinal(IDataRecord reader, string columnName)
    {
        try
        {
            return reader.GetOrdinal(columnName);
        }
        catch (IndexOutOfRangeException)
        {
            return -1;
        }
    }

    private async Task<int> ExecuteNonQueryInternalAsync(string sql, List<NpgsqlParameter> parameters, CancellationToken ct, string modelName = "", string operation = "Execute")
    {
        await using var dbConn = await _connectionProvider.OpenAsync(ct).ConfigureAwait(false);
        if (dbConn is not NpgsqlConnection conn)
        {
            throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
        }

        return await ExecuteNonQueryInternalAsync(sql, parameters, conn, null, ct, modelName, operation).ConfigureAwait(false);
    }

    private async Task<int> ExecuteNonQueryInternalAsync(string sql, List<NpgsqlParameter> parameters, NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct, string modelName = "", string operation = "Execute")
    {
        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn)
            {
                Transaction = tx
            };
            foreach (var p in parameters)
            {
                cmd.Parameters.Add(p);
            }
            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new UniqueConstraintViolationException(modelName, operation, ex.ConstraintName, ex);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
        {
            throw new ForeignKeyViolationException(modelName, operation, ex.ConstraintName, ex);
        }
    }

    private async Task<int> ExecuteScalarAsync(string sql, List<NpgsqlParameter> parameters, NpgsqlConnection? conn, NpgsqlTransaction? tx, CancellationToken ct, string modelName, string operation)
    {
        if (conn is null)
        {
            await using var dbConn = await _connectionProvider.OpenAsync(ct).ConfigureAwait(false);
            if (dbConn is not NpgsqlConnection npgConn)
            {
                throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
            }
            return await ExecuteScalarAsync(sql, parameters, npgConn, null, ct, modelName, operation).ConfigureAwait(false);
        }

        await using var cmd = new NpgsqlCommand(sql, conn)
        {
            Transaction = tx
        };
        foreach (var p in parameters)
        {
            cmd.Parameters.Add(p);
        }

        try
        {
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            if (result is null || result is DBNull)
            {
                return 0;
            }
            return Convert.ToInt32(result, CultureInfo.InvariantCulture);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new UniqueConstraintViolationException(modelName, operation, ex.ConstraintName, ex);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
        {
            throw new ForeignKeyViolationException(modelName, operation, ex.ConstraintName, ex);
        }
    }

    /// <summary>
    /// Executes a single-row aggregate query and materializes it into the generated aggregate result type.
    /// </summary>
    private async Task<object?> ExecuteAggregateInternalAsync(Type resultType, string sql, List<NpgsqlParameter> parameters, NpgsqlConnection? connOverride, NpgsqlTransaction? txOverride, CancellationToken ct, string operation)
    {
        var ownsConnection = connOverride is null;
        await using var ownedConn = ownsConnection ? await _connectionProvider.OpenAsync(ct).ConfigureAwait(false) : null;
        var conn = connOverride ?? ownedConn as NpgsqlConnection;
        if (conn is null)
        {
            throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
        }

        try
        {
            await using var cmd = new NpgsqlCommand(sql, conn)
            {
                Transaction = txOverride
            };
            foreach (var p in parameters)
            {
                cmd.Parameters.Add(p);
            }

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return null;
            }

            var instance = Activator.CreateInstance(resultType) ?? throw new InvalidOperationException($"Failed to create aggregate result instance of '{resultType.FullName}'.");
            PopulateAggregateResult(instance, reader);
            return instance;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new UniqueConstraintViolationException(resultType.Name, operation, ex.ConstraintName, ex);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.ForeignKeyViolation)
        {
            throw new ForeignKeyViolationException(resultType.Name, operation, ex.ConstraintName, ex);
        }
    }

    /// <summary>
    /// Projects reader columns (count/min/max/avg/sum) into the aggregate result object graph.
    /// </summary>
    private static void PopulateAggregateResult(object aggregateResult, IDataRecord reader)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            var columnName = reader.GetName(i);
            if (reader.IsDBNull(i))
            {
                continue;
            }

            var value = reader.GetValue(i);
            if (string.Equals(columnName, "count", StringComparison.OrdinalIgnoreCase))
            {
                SetPropertyValue(aggregateResult, "Count", Convert.ToInt32(value, CultureInfo.InvariantCulture));
                continue;
            }

            if (columnName.StartsWith("min_", StringComparison.OrdinalIgnoreCase))
            {
                ApplyNestedAggregateValue(aggregateResult, "Min", columnName[4..], value);
                continue;
            }

            if (columnName.StartsWith("max_", StringComparison.OrdinalIgnoreCase))
            {
                ApplyNestedAggregateValue(aggregateResult, "Max", columnName[4..], value);
                continue;
            }

            if (columnName.StartsWith("avg_", StringComparison.OrdinalIgnoreCase))
            {
                ApplyNestedAggregateValue(aggregateResult, "Avg", columnName[4..], value);
                continue;
            }

            if (columnName.StartsWith("sum_", StringComparison.OrdinalIgnoreCase))
            {
                ApplyNestedAggregateValue(aggregateResult, "Sum", columnName[4..], value);
            }
        }
    }

    private static void PopulateGroupByResult(ModelMetadata meta, object groupResult, IDataRecord reader)
    {
        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var ordinal = TryGetOrdinal(reader, field.Name);
            if (ordinal < 0 || reader.IsDBNull(ordinal))
            {
                continue;
            }

            var value = reader.GetValue(ordinal);
            SetPropertyValue(groupResult, field.Name, value);
        }

        PopulateAggregateResult(groupResult, reader);
    }

    private static void ApplyNestedAggregateValue(object root, string containerProperty, string fieldName, object value)
    {
        var containerProp = root.GetType().GetProperty(containerProperty, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (containerProp is null || !containerProp.CanWrite)
        {
            return;
        }

        var current = containerProp.GetValue(root);
        if (current is null)
        {
            current = Activator.CreateInstance(containerProp.PropertyType) ?? throw new InvalidOperationException($"Failed to create aggregate container '{containerProp.PropertyType.FullName}'.");
        }

        SetPropertyValue(current, fieldName, value);
        containerProp.SetValue(root, current);
    }

    private static void SetPropertyValue(object target, string propertyName, object value)
    {
        var prop = target.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (prop is null || !prop.CanWrite)
        {
            return;
        }

        var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
        var converted = ConvertAggregateValue(value, targetType);
        prop.SetValue(target, converted);
    }

    private static object? ConvertAggregateValue(object value, Type targetType)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        if (targetType.IsInstanceOfType(value))
        {
            return value;
        }

        if (targetType == typeof(Guid))
        {
            return value is Guid guid ? guid : Guid.Parse(value.ToString() ?? string.Empty);
        }

        if (targetType.IsEnum)
        {
            return Enum.ToObject(targetType, value);
        }

        if (value is IConvertible)
        {
            return Convert.ChangeType(value, targetType, CultureInfo.InvariantCulture);
        }

        return value;
    }

    private static object? GetProperty(object target, string name)
    {
        return target.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance)?.GetValue(target);
    }

    private static object GetRequiredProperty(object target, string name)
    {
        var value = GetProperty(target, name);
        if (value is null)
        {
            throw new InvalidOperationException($"Required property '{name}' was not provided on type '{target.GetType().Name}'.");
        }
        return value;
    }

    private static Dictionary<FieldMetadata, object> ExtractRelationPayloads(ModelMetadata meta, object data)
    {
        var payloads = new Dictionary<FieldMetadata, object>();

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Relation))
        {
            var prop = data.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            var value = prop?.GetValue(data);
            if (value is not null)
            {
                payloads[field] = value;
            }
        }

        return payloads;
    }

    private static void EnsureSelectIncludeExclusivity(object? select, object? include)
    {
        if (select is not null && include is not null)
        {
            throw new NotSupportedException("Select and Include cannot be combined in this executor; prefer Include to fetch relations or Select to shape scalars.");
        }
    }

    private static void EnsureSelectIncludeOmitExclusivity(object? select, object? include, object? omit)
    {
        if ((select is not null && include is not null) || (select is not null && omit is not null) || (include is not null && omit is not null))
        {
            throw new NotSupportedException("Select, Include, and Omit cannot be combined; provide at most one.");
        }
    }

    private static IReadOnlyList<string> BuildSelectColumns(ModelMetadata meta, object? selectObj, object? omitObj = null, bool includePrimaryKey = false)
    {
        var selected = new List<string>();
        if (selectObj is null && omitObj is null)
        {
            selected.AddRange(meta.Fields
                .Where(f => f.Kind == FieldKind.Scalar)
                .Select(f => f.Name));
        }
        else if (selectObj is not null)
        {
            foreach (var field in meta.Fields)
            {
                if (field.Kind != FieldKind.Scalar)
                {
                    continue;
                }

                var prop = selectObj.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
                if (prop is null)
                {
                    continue;
                }
                var val = prop.GetValue(selectObj) as bool?;
                if (val == true)
                {
                    selected.Add(field.Name);
                }
            }

            if (selected.Count == 0)
            {
                selected.AddRange(meta.Fields.Where(f => f.Kind == FieldKind.Scalar).Select(f => f.Name));
            }
        }
        else if (omitObj is not null)
        {
            foreach (var field in meta.Fields)
            {
                if (field.Kind != FieldKind.Scalar)
                {
                    continue;
                }

                var prop = omitObj.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
                var val = prop?.GetValue(omitObj) as bool?;
                if (val != true)
                {
                    selected.Add(field.Name);
                }
            }

            if (selected.Count == 0)
            {
                selected.AddRange(meta.Fields.Where(f => f.Kind == FieldKind.Scalar).Select(f => f.Name));
            }
        }

        if (includePrimaryKey && meta.PrimaryKey is { } pk)
        {
            foreach (var pkField in pk.Fields)
            {
                if (!selected.Contains(pkField, StringComparer.Ordinal))
                {
                    selected.Add(pkField);
                }
            }
        }

        return selected;
    }

    private List<PlanningIncludePlan> BuildIncludePlans(ModelMetadata meta, object? includeObj)
    {
        var aliasCounter = 0;
        return BuildIncludePlans(meta, includeObj, "t0", ref aliasCounter, 0);
    }

    private List<PlanningIncludePlan> BuildIncludePlans(ModelMetadata meta, object? includeObj, string parentAlias, ref int aliasCounter, int depth)
    {
        var plans = new List<PlanningIncludePlan>();
        if (includeObj is null)
        {
            return plans;
        }

        if (depth >= _maxNestingDepth)
        {
            throw new NotSupportedException($"Maximum include nesting depth of {_maxNestingDepth} was exceeded.");
        }

        foreach (var relationField in meta.Fields.Where(f => f.Kind == FieldKind.Relation))
        {
            var prop = includeObj.GetType().GetProperty(relationField.Name, BindingFlags.Public | BindingFlags.Instance);
            var value = prop?.GetValue(includeObj);
            if (value is null)
            {
                continue;
            }

            var childMeta = GetModelMetadata(relationField.ClrType);
            var childSelect = GetProperty(value, "Select");
            var childOmit = GetProperty(value, "Omit");
            EnsureSelectIncludeOmitExclusivity(childSelect, null, childOmit);
            var columns = BuildSelectColumns(childMeta, childSelect, childOmit, includePrimaryKey: true);
            var mapping = ResolveRelationMapping(meta, childMeta, relationField);
            var childAlias = $"t{++aliasCounter}";
            var childPrefix = $"{childAlias}__";
            var nested = BuildIncludePlans(childMeta, value, childAlias, ref aliasCounter, depth + 1);

            plans.Add(new PlanningIncludePlan(
                relationField.Name,
                childMeta,
                childAlias,
                parentAlias,
                childPrefix,
                relationField.IsList,
                mapping.ParentColumns,
                mapping.ChildColumns,
                nested,
                columns));
        }

        return plans;
    }

    private RelationMapping ResolveRelationMapping(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField)
    {
        var fkInParent = parentMeta.ForeignKeys.Where(fk => string.Equals(fk.PrincipalModel, childMeta.Name, StringComparison.Ordinal)).ToList();
        var fkInChild = childMeta.ForeignKeys.Where(fk => string.Equals(fk.PrincipalModel, parentMeta.Name, StringComparison.Ordinal)).ToList();

        if (relationField.IsList)
        {
            if (fkInChild.Count == 1)
            {
                var fk = fkInChild[0];
                return new RelationMapping(fk.PrincipalFields, fk.LocalFields);
            }

            throw new NotSupportedException($"Could not resolve collection relation mapping for '{relationField.Name}' from '{parentMeta.Name}' to '{childMeta.Name}'.");
        }

        if (fkInParent.Count == 1)
        {
            var fk = fkInParent[0];
            return new RelationMapping(fk.LocalFields, fk.PrincipalFields);
        }

        if (fkInChild.Count == 1)
        {
            var fk = fkInChild[0];
            return new RelationMapping(fk.PrincipalFields, fk.LocalFields);
        }

        throw new NotSupportedException($"Could not resolve relation mapping for '{relationField.Name}' between '{parentMeta.Name}' and '{childMeta.Name}'.");
    }

    private RelationForeignKey ResolveRelationForeignKey(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField)
    {
        if (relationField.IsList)
        {
            if (TryResolveChildForeignKey(parentMeta, childMeta, relationField, out var childFk))
            {
                return new RelationForeignKey(ForeignKeyOwner.Child, childFk!);
            }

            throw new NotSupportedException($"Relation '{relationField.Name}' on '{parentMeta.Name}' must be backed by a child-owned foreign key.");
        }

        var childOwned = TryResolveChildForeignKey(parentMeta, childMeta, relationField, out var childOwnedFk);
        var parentOwned = TryResolveParentForeignKey(parentMeta, childMeta, relationField, out var parentOwnedFk);

        if (childOwned && parentOwned)
        {
            throw new NotSupportedException($"Relation '{relationField.Name}' between '{parentMeta.Name}' and '{childMeta.Name}' is ambiguous (FK on both sides).");
        }

        if (childOwned)
        {
            return new RelationForeignKey(ForeignKeyOwner.Child, childOwnedFk!);
        }

        if (parentOwned)
        {
            return new RelationForeignKey(ForeignKeyOwner.Parent, parentOwnedFk!);
        }

        throw new NotSupportedException($"Could not resolve foreign key ownership for relation '{relationField.Name}' between '{parentMeta.Name}' and '{childMeta.Name}'.");
    }

    private static bool TryResolveChildForeignKey(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField, out ForeignKeyMetadata? fk)
    {
        fk = childMeta.ForeignKeys
            .FirstOrDefault(x => string.Equals(x.PrincipalModel, parentMeta.Name, StringComparison.Ordinal)
                                  && (x.RelationName is null || string.Equals(x.RelationName, relationField.Name, StringComparison.Ordinal)));
        return fk is not null;
    }

    private static bool TryResolveParentForeignKey(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField, out ForeignKeyMetadata? fk)
    {
        fk = parentMeta.ForeignKeys
            .FirstOrDefault(x => string.Equals(x.PrincipalModel, childMeta.Name, StringComparison.Ordinal)
                                  && (x.RelationName is null || string.Equals(x.RelationName, relationField.Name, StringComparison.Ordinal)));
        return fk is not null;
    }

    private static void AppendSelectColumns(List<string> fragments, ModelMetadata meta, IReadOnlyList<string> columns, string alias, string prefix)
    {
        foreach (var column in columns)
        {
            var aliasLabel = string.IsNullOrEmpty(prefix) ? QuoteIdentifier(column) : QuoteIdentifier($"{prefix}{column}");
            fragments.Add($"\"{alias}\".{QuoteIdentifier(column)} AS {aliasLabel}");
        }
    }

    private void AppendIncludeSelectColumns(List<string> fragments, IReadOnlyList<PlanningIncludePlan> includes)
    {
        foreach (var include in includes)
        {
            AppendSelectColumns(fragments, include.Meta, include.Columns, include.Alias, include.ColumnPrefix);
            AppendIncludeSelectColumns(fragments, include.Children);
        }
    }

    private static IReadOnlyList<string> BuildJoinFragments(IReadOnlyList<PlanningIncludePlan> includes)
    {
        var joins = new List<string>();
        foreach (var include in includes)
        {
            var conditions = include.ParentColumns.Zip(include.ChildColumns, (parentCol, childCol) => $"\"{include.ParentAlias}\".{QuoteIdentifier(parentCol)} = \"{include.Alias}\".{QuoteIdentifier(childCol)}");
            joins.Add($"LEFT JOIN {QuoteIdentifier(include.Meta.Name)} AS \"{include.Alias}\" ON {string.Join(" AND ", conditions)}");
            var nested = BuildJoinFragments(include.Children);
            joins.AddRange(nested);
        }

        return joins;
    }

    private static string BuildWhereUnique(ModelMetadata meta, object whereObj, List<NpgsqlParameter> parameters, ParameterContext paramCtx, string tableAlias)
    {
        ArgumentNullException.ThrowIfNull(meta);
        ArgumentNullException.ThrowIfNull(whereObj);
        ArgumentNullException.ThrowIfNull(paramCtx);

        var clauses = new List<string>();
        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var prop = whereObj.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            if (prop is null)
            {
                continue;
            }

            var value = prop.GetValue(whereObj);
            if (value is null)
            {
                continue;
            }

            var paramName = paramCtx.Next();
            clauses.Add($"\"{tableAlias}\".{QuoteIdentifier(field.Name)} = {paramName}");
            parameters.Add(CreateParameter(paramName, value, field.ClrType));
        }

        if (clauses.Count == 0)
        {
            throw new InvalidOperationException($"Where predicate for '{meta.Name}' produced no filters; refusing to run a non-unique mutation.");
        }

        return string.Join(" AND ", clauses);
    }

    private static void EnsureWhereValuesPresentOnCreate(ModelMetadata meta, object whereObj, object createObj, string relationName)
    {
        var whereType = whereObj.GetType();
        var createType = createObj.GetType();

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var whereProp = whereType.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            if (whereProp is null)
            {
                continue;
            }

            var whereValue = whereProp.GetValue(whereObj);
            if (whereValue is null)
            {
                continue;
            }

            var createProp = createType.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            if (createProp is null)
            {
                throw new InvalidOperationException($"ConnectOrCreate for relation '{relationName}' requires create payload to include unique field '{field.Name}'.");
            }

            var currentValue = createProp.GetValue(createObj);
            if (IsUnsetValue(currentValue, createProp.PropertyType))
            {
                if (!Equals(currentValue, whereValue))
                {
                    createProp.SetValue(createObj, whereValue);
                }
                continue;
            }

            if (!Equals(currentValue, whereValue))
            {
                throw new InvalidOperationException($"ConnectOrCreate for relation '{relationName}' must use the same value for unique field '{field.Name}' in Where and Create.");
            }
        }
    }

    private static bool IsUnsetValue(object? value, Type propertyType)
    {
        if (value is null)
        {
            return true;
        }

        var underlying = Nullable.GetUnderlyingType(propertyType);
        if (underlying is not null)
        {
            // For nullable value types, any non-null value counts as set, even if it equals the underlying default.
            return false;
        }

        if (!propertyType.IsValueType)
        {
            return false;
        }

        var defaultValue = Activator.CreateInstance(propertyType);
        return Equals(value, defaultValue);
    }

    private static object? GenerateValueForUnsetPrimaryKey(Type propertyType)
    {
        var targetType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        if (targetType == typeof(Guid))
        {
            return Guid.NewGuid();
        }

        return null;
    }

    private static Guid GenerateGuidV7()
    {
        Span<byte> bytes = stackalloc byte[16];
        var unixTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        bytes[0] = (byte)((unixTimeMs >> 40) & 0xFF);
        bytes[1] = (byte)((unixTimeMs >> 32) & 0xFF);
        bytes[2] = (byte)((unixTimeMs >> 24) & 0xFF);
        bytes[3] = (byte)((unixTimeMs >> 16) & 0xFF);
        bytes[4] = (byte)((unixTimeMs >> 8) & 0xFF);
        bytes[5] = (byte)(unixTimeMs & 0xFF);

        RandomNumberGenerator.Fill(bytes.Slice(6));

        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x70); // version 7
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80); // variant 10xx

        return new Guid(bytes);
    }

    private static JsonElement ParseJsonDefault(string? raw)
    {
        var payload = string.IsNullOrWhiteSpace(raw) ? "null" : raw;
        using var doc = JsonDocument.Parse(payload);
        return doc.RootElement.Clone();
    }

    private static object? ConvertStaticDefault(string? raw, Type propertyType)
    {
        var targetType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        if (targetType == typeof(string))
        {
            return raw ?? string.Empty;
        }

        if (targetType == typeof(Guid))
        {
            return Guid.Parse(raw ?? Guid.Empty.ToString());
        }

        if (targetType == typeof(int))
        {
            return int.Parse(raw ?? "0", CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(long))
        {
            return long.Parse(raw ?? "0", CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(double))
        {
            return double.Parse(raw ?? "0", CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(decimal))
        {
            return decimal.Parse(raw ?? "0", CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(bool))
        {
            return bool.Parse(raw ?? "false");
        }

        if (targetType == typeof(DateTime))
        {
            // Accept friendly placeholders like "now"/"now()" in addition to ISO strings.
            if (string.IsNullOrWhiteSpace(raw))
            {
                return DateTime.UtcNow;
            }

            if (raw.Equals("now", StringComparison.OrdinalIgnoreCase) || raw.StartsWith("now", StringComparison.OrdinalIgnoreCase))
            {
                return DateTime.UtcNow;
            }

            if (DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
            {
                return parsed;
            }

            return DateTime.UtcNow;
        }

        if (targetType == typeof(JsonElement))
        {
            return ParseJsonDefault(raw);
        }

        if (IsJsonWrapperType(targetType))
        {
            var element = ParseJsonDefault(raw);
            return CreateJsonWrapper(targetType, element);
        }

        if (targetType == typeof(byte[]))
        {
            return raw is null ? Array.Empty<byte>() : Convert.FromBase64String(raw);
        }

        return raw;
    }

    private string BuildWhere(ModelMetadata meta, object? whereObj, List<NpgsqlParameter> parameters, ParameterContext paramCtx, string tableAlias, ref AliasContext aliasCtx)
    {
        if (whereObj is null)
        {
            return string.Empty;
        }

        var clauses = new List<string>();
        var type = whereObj.GetType();

        var andProp = type.GetProperty("AND");
        if (andProp?.GetValue(whereObj) is System.Collections.IEnumerable andConditions)
        {
            var parts = new List<string>();
            foreach (var item in andConditions)
            {
                var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
                if (!string.IsNullOrWhiteSpace(fragment))
                {
                    parts.Add(fragment);
                }
            }
            if (parts.Count > 0)
            {
                clauses.Add($"( {string.Join(" AND ", parts)} )");
            }
        }

        var orProp = type.GetProperty("OR");
        if (orProp?.GetValue(whereObj) is System.Collections.IEnumerable orConditions)
        {
            var parts = new List<string>();
            foreach (var item in orConditions)
            {
                var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
                if (!string.IsNullOrWhiteSpace(fragment))
                {
                    parts.Add(fragment);
                }
            }
            if (parts.Count > 0)
            {
                clauses.Add($"( {string.Join(" OR ", parts)} )");
            }
        }

        var xorProp = type.GetProperty("XOR");
        if (xorProp?.GetValue(whereObj) is System.Collections.IEnumerable xorConditions)
        {
            var parts = new List<string>();
            foreach (var item in xorConditions)
            {
                var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
                if (!string.IsNullOrWhiteSpace(fragment))
                {
                    parts.Add(fragment);
                }
            }

            if (parts.Count > 0)
            {
                var orTerms = new List<string>(parts.Count);
                for (int i = 0; i < parts.Count; i++)
                {
                    var positives = parts[i];
                    var negatives = parts.Where((_, idx) => idx != i).Select(p => $"NOT ({p})");
                    var term = negatives.Any() ? $"({positives}) AND {string.Join(" AND ", negatives)}" : positives;
                    orTerms.Add($"( {term} )");
                }

                clauses.Add($"( {string.Join(" OR ", orTerms)} )");
            }
        }

        var notProp = type.GetProperty("NOT");
        if (notProp?.GetValue(whereObj) is System.Collections.IEnumerable notConditions)
        {
            var parts = new List<string>();
            foreach (var item in notConditions)
            {
                var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
                if (!string.IsNullOrWhiteSpace(fragment))
                {
                    parts.Add($"NOT ({fragment})");
                }
            }
            if (parts.Count > 0)
            {
                clauses.Add(string.Join(" AND ", parts));
            }
        }

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var prop = type.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            if (prop is null)
            {
                continue;
            }

            var value = prop.GetValue(whereObj);
            if (value is null)
            {
                continue;
            }

            var clause = BuildScalarFilterClause(field, value, parameters, paramCtx, tableAlias);
            if (!string.IsNullOrWhiteSpace(clause))
            {
                clauses.Add(clause);
            }
        }

        foreach (var relation in meta.Fields.Where(f => f.Kind == FieldKind.Relation))
        {
            var prop = type.GetProperty(relation.Name, BindingFlags.Public | BindingFlags.Instance);
            if (prop is null)
            {
                continue;
            }

            var value = prop.GetValue(whereObj);
            if (value is null)
            {
                continue;
            }

            var clause = BuildRelationFilterClause(meta, relation, value, parameters, paramCtx, tableAlias, ref aliasCtx);
            if (!string.IsNullOrWhiteSpace(clause))
            {
                clauses.Add(clause);
            }
        }

        return string.Join(" AND ", clauses);
    }

    private static string BuildScalarFilterClause(FieldMetadata field, object filterValue, List<NpgsqlParameter> parameters, ParameterContext paramCtx, string tableAlias)
    {
        if (IsJsonClrType(field.ClrType))
        {
            return BuildJsonFilterClause(field, filterValue, parameters, paramCtx, tableAlias);
        }

        if (IsSimpleValue(filterValue))
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, filterValue, field.ClrType));
            return $"\"{tableAlias}\".{QuoteIdentifier(field.Name)} = {paramName}";
        }

        var clauses = new List<string>();
        var type = filterValue.GetType();
        var column = $"\"{tableAlias}\".{QuoteIdentifier(field.Name)}";

        void AddIfNotNull(string propName, Func<string> builder)
        {
            var prop = type.GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
            var val = prop?.GetValue(filterValue);
            if (val is not null)
            {
                var built = builder();
                if (!string.IsNullOrWhiteSpace(built))
                {
                    clauses.Add(built);
                }
            }
        }

        AddIfNotNull("Equals", () =>
        {
            var paramName = paramCtx.Next();
            var value = type.GetProperty("Equals")!.GetValue(filterValue);

            if (IsStringClrType(field.ClrType))
            {
                var (likeOp, comparison) = BuildStringComparisonFragments(column, paramName, value, ResolveStringMode(type, filterValue));
                parameters.Add(CreateParameter(paramName, comparison, field.ClrType));
                return likeOp;
            }

            parameters.Add(CreateParameter(paramName, GetParameterValue(value), field.ClrType));
            return $"{column} = {paramName}";
        });

        AddIfNotNull("In", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("In")!.GetValue(filterValue)), field.ClrType));
            return $"{column} = ANY({paramName})";
        });

        AddIfNotNull("NotIn", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("NotIn")!.GetValue(filterValue)), field.ClrType));
            return $"NOT ({column} = ANY({paramName}))";
        });

        AddIfNotNull("Gt", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Gt")!.GetValue(filterValue)), field.ClrType));
            return $"{column} > {paramName}";
        });

        AddIfNotNull("Gte", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Gte")!.GetValue(filterValue)), field.ClrType));
            return $"{column} >= {paramName}";
        });

        AddIfNotNull("Lt", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Lt")!.GetValue(filterValue)), field.ClrType));
            return $"{column} < {paramName}";
        });

        AddIfNotNull("Lte", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Lte")!.GetValue(filterValue)), field.ClrType));
            return $"{column} <= {paramName}";
        });

        AddIfNotNull("Contains", () =>
        {
            var paramName = paramCtx.Next();
            var like = ResolveStringLike(column, paramName, type.GetProperty("Contains")!.GetValue(filterValue), ResolveStringMode(type, filterValue), wrapBoth: true, startsWith: false, endsWith: false);
            parameters.Add(CreateParameter(paramName, like.Pattern, field.ClrType));
            return like.Clause;
        });

        AddIfNotNull("StartsWith", () =>
        {
            var paramName = paramCtx.Next();
            var like = ResolveStringLike(column, paramName, type.GetProperty("StartsWith")!.GetValue(filterValue), ResolveStringMode(type, filterValue), wrapBoth: false, startsWith: true, endsWith: false);
            parameters.Add(CreateParameter(paramName, like.Pattern, field.ClrType));
            return like.Clause;
        });

        AddIfNotNull("EndsWith", () =>
        {
            var paramName = paramCtx.Next();
            var like = ResolveStringLike(column, paramName, type.GetProperty("EndsWith")!.GetValue(filterValue), ResolveStringMode(type, filterValue), wrapBoth: false, startsWith: false, endsWith: true);
            parameters.Add(CreateParameter(paramName, like.Pattern, field.ClrType));
            return like.Clause;
        });

        AddIfNotNull("Not", () =>
        {
            var notVal = type.GetProperty("Not")!.GetValue(filterValue);
            var clause = BuildScalarFilterClause(field, notVal!, parameters, paramCtx, tableAlias);
            return $"NOT ({clause})";
        });

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonFilterClause(FieldMetadata field, object filterValue, List<NpgsqlParameter> parameters, ParameterContext paramCtx, string tableAlias)
    {
        var column = $"\"{tableAlias}\".{QuoteIdentifier(field.Name)}";

        if (IsSimpleValue(filterValue))
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, filterValue, "Json"));
            return $"{column} = {paramName}::jsonb";
        }

        var clauses = new List<string>();
        var type = filterValue.GetType();

        void AddIfNotNull(string propName, Func<string> builder)
        {
            var prop = type.GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
            var val = prop?.GetValue(filterValue);
            if (val is not null)
            {
                clauses.Add(builder());
            }
        }

        AddIfNotNull("Equals", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, type.GetProperty("Equals")!.GetValue(filterValue), "Json"));
            return $"{column} = {paramName}::jsonb";
        });

        AddIfNotNull("Not", () =>
        {
            var notVal = type.GetProperty("Not")!.GetValue(filterValue)!;
            var clause = BuildJsonFilterClause(field, notVal, parameters, paramCtx, tableAlias);
            return $"NOT ({clause})";
        });

        AddIfNotNull("Path", () =>
        {
            var pathVal = type.GetProperty("Path")!.GetValue(filterValue)!;
            return BuildJsonPathFilterClause(column, pathVal, parameters, paramCtx);
        });

        AddIfNotNull("Array", () =>
        {
            var arrayVal = type.GetProperty("Array")!.GetValue(filterValue)!;
            return BuildJsonArrayFilterClause(BuildJsonPathExpression(column, Array.Empty<string>()), arrayVal, parameters, paramCtx);
        });

        AddIfNotNull("String", () =>
        {
            var stringVal = type.GetProperty("String")!.GetValue(filterValue)!;
            return BuildJsonStringFilterClause(column, Array.Empty<string>(), stringVal, parameters, paramCtx);
        });

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonPathFilterClause(string column, object pathFilter, List<NpgsqlParameter> parameters, ParameterContext paramCtx)
    {
        var type = pathFilter.GetType();
        var segmentsObj = type.GetProperty("Segments")?.GetValue(pathFilter) as System.Collections.IEnumerable;
        var segments = segmentsObj?.Cast<object?>().Select(s => s?.ToString() ?? string.Empty).Where(s => s is not null).ToList() ?? new List<string>();

        var jsonExpr = BuildJsonPathExpression(column, segments);

        var clauses = new List<string>();

        void AddIfNotNull(string propName, Func<string> builder)
        {
            var prop = type.GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
            var val = prop?.GetValue(pathFilter);
            if (val is not null)
            {
                var built = builder();
                if (!string.IsNullOrWhiteSpace(built))
                {
                    clauses.Add(built);
                }
            }
        }

        AddIfNotNull("Equals", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, type.GetProperty("Equals")!.GetValue(pathFilter), "Json"));
            return $"{jsonExpr} = {paramName}::jsonb";
        });

        AddIfNotNull("Not", () =>
        {
            var notVal = type.GetProperty("Not")!.GetValue(pathFilter)!;
            var clause = BuildJsonPathFilterClause(column, notVal, parameters, paramCtx);
            return $"NOT ({clause})";
        });

        AddIfNotNull("Array", () =>
        {
            var arrayVal = type.GetProperty("Array")!.GetValue(pathFilter)!;
            return BuildJsonArrayFilterClause(jsonExpr, arrayVal, parameters, paramCtx);
        });

        AddIfNotNull("String", () =>
        {
            var stringVal = type.GetProperty("String")!.GetValue(pathFilter)!;
            return BuildJsonStringFilterClause(column, segments, stringVal, parameters, paramCtx);
        });

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonArrayFilterClause(string jsonExpr, object arrayFilter, List<NpgsqlParameter> parameters, ParameterContext paramCtx)
    {
        var type = arrayFilter.GetType();
        var clauses = new List<string>();
        var ensureArray = $"jsonb_typeof({jsonExpr}) = 'array'";

        void AddIfNotNull(string propName, Func<string> builder)
        {
            var prop = type.GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
            var val = prop?.GetValue(arrayFilter);
            if (val is not null)
            {
                var built = builder();
                if (!string.IsNullOrWhiteSpace(built))
                {
                    clauses.Add(built);
                }
            }
        }

        AddIfNotNull("Has", () =>
        {
            var paramName = paramCtx.Next();
            var payload = WrapArrayPayload(type.GetProperty("Has")!.GetValue(arrayFilter));
            parameters.Add(CreateParameter(paramName, payload, "Json"));
            return $"{ensureArray} AND {jsonExpr} @> {paramName}::jsonb";
        });

        AddIfNotNull("HasEvery", () =>
        {
            var values = type.GetProperty("HasEvery")!.GetValue(arrayFilter) as System.Collections.IEnumerable
                         ?? throw new QueryValidationException("", QueryType.FindMany, "HasEvery expects an array of JSON values.");
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, BuildArrayPayload(values), "Json"));
            return $"{ensureArray} AND {jsonExpr} @> {paramName}::jsonb";
        });

        AddIfNotNull("HasSome", () =>
        {
            var values = type.GetProperty("HasSome")!.GetValue(arrayFilter) as System.Collections.IEnumerable
                         ?? throw new QueryValidationException("", QueryType.FindMany, "HasSome expects an array of JSON values.");
            var predicates = new List<string>();
            foreach (var val in values)
            {
                var paramName = paramCtx.Next();
                parameters.Add(CreateParameter(paramName, WrapArrayPayload(val), "Json"));
                predicates.Add($"{jsonExpr} @> {paramName}::jsonb");
            }

            if (predicates.Count == 0)
            {
                throw new QueryValidationException("", QueryType.FindMany, "HasSome expects at least one element.");
            }

            return $"{ensureArray} AND (" + string.Join(" OR ", predicates) + ")";
        });

        AddIfNotNull("Length", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, type.GetProperty("Length")!.GetValue(arrayFilter)));
            return $"{ensureArray} AND jsonb_array_length({jsonExpr}) = {paramName}";
        });

        AddIfNotNull("IsEmpty", () =>
        {
            var isEmpty = type.GetProperty("IsEmpty")!.GetValue(arrayFilter) as bool?;
            if (isEmpty is null)
            {
                return string.Empty;
            }
            return isEmpty.Value
                ? $"{ensureArray} AND jsonb_array_length({jsonExpr}) = 0"
                : $"{ensureArray} AND jsonb_array_length({jsonExpr}) > 0";
        });

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonStringFilterClause(string column, IReadOnlyList<string> segments, object stringFilter, List<NpgsqlParameter> parameters, ParameterContext paramCtx)
    {
        var textExpr = BuildJsonTextExpression(column, segments);
        var clauses = new List<string>();
        var type = stringFilter.GetType();
        var mode = ResolveStringMode(type, stringFilter);

        void AddIfNotNull(string propName, Func<string> builder)
        {
            var prop = type.GetProperty(propName, BindingFlags.Public | BindingFlags.Instance);
            var val = prop?.GetValue(stringFilter);
            if (val is not null)
            {
                var built = builder();
                if (!string.IsNullOrWhiteSpace(built))
                {
                    clauses.Add(built);
                }
            }
        }

        AddIfNotNull("Equals", () =>
        {
            var paramName = paramCtx.Next();
            var (clause, parameter) = BuildStringComparisonFragments(textExpr, paramName, type.GetProperty("Equals")!.GetValue(stringFilter), mode);
            parameters.Add(CreateParameter(paramName, parameter));
            return clause;
        });

        AddIfNotNull("Contains", () =>
        {
            var paramName = paramCtx.Next();
            var like = ResolveStringLike(textExpr, paramName, type.GetProperty("Contains")!.GetValue(stringFilter), mode, wrapBoth: true, startsWith: false, endsWith: false);
            parameters.Add(CreateParameter(paramName, like.Pattern));
            return like.Clause;
        });

        AddIfNotNull("StartsWith", () =>
        {
            var paramName = paramCtx.Next();
            var like = ResolveStringLike(textExpr, paramName, type.GetProperty("StartsWith")!.GetValue(stringFilter), mode, wrapBoth: false, startsWith: true, endsWith: false);
            parameters.Add(CreateParameter(paramName, like.Pattern));
            return like.Clause;
        });

        AddIfNotNull("EndsWith", () =>
        {
            var paramName = paramCtx.Next();
            var like = ResolveStringLike(textExpr, paramName, type.GetProperty("EndsWith")!.GetValue(stringFilter), mode, wrapBoth: false, startsWith: false, endsWith: true);
            parameters.Add(CreateParameter(paramName, like.Pattern));
            return like.Clause;
        });

        AddIfNotNull("In", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("In")!.GetValue(stringFilter))));
            return $"{textExpr} = ANY({paramName})";
        });

        AddIfNotNull("NotIn", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("NotIn")!.GetValue(stringFilter))));
            return $"NOT ({textExpr} = ANY({paramName}))";
        });

        AddIfNotNull("Not", () =>
        {
            var notVal = type.GetProperty("Not")!.GetValue(stringFilter)!;
            return $"NOT ({BuildJsonStringFilterClause(column, segments, notVal, parameters, paramCtx)})";
        });

        AddIfNotNull("Gt", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Gt")!.GetValue(stringFilter))));
            return $"{textExpr} > {paramName}";
        });

        AddIfNotNull("Gte", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Gte")!.GetValue(stringFilter))));
            return $"{textExpr} >= {paramName}";
        });

        AddIfNotNull("Lt", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Lt")!.GetValue(stringFilter))));
            return $"{textExpr} < {paramName}";
        });

        AddIfNotNull("Lte", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(CreateParameter(paramName, GetParameterValue(type.GetProperty("Lte")!.GetValue(stringFilter))));
            return $"{textExpr} <= {paramName}";
        });

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonPathExpression(string column, IReadOnlyList<string> segments)
    {
        if (segments.Count == 0)
        {
            return column;
        }

        return $"{column} #> '{BuildPathLiteral(segments)}'";
    }

    private static string BuildJsonTextExpression(string column, IReadOnlyList<string> segments)
    {
        var literal = BuildPathLiteral(segments);
        return $"{column} #>> '{literal}'";
    }

    private static string BuildPathLiteral(IReadOnlyList<string> segments)
    {
        if (segments.Count == 0)
        {
            return "{}";
        }

        var escaped = segments.Select(s => $"\"{s.Replace("\"", "\"\"").Replace("'", "''")}\"");
        return "{" + string.Join(",", escaped) + "}";
    }

    private enum StringComparisonModeValue
    {
        Default,
        Insensitive,
        Sensitive
    }

    private static StringComparisonModeValue ResolveStringMode(Type filterType, object instance)
    {
        var prop = filterType.GetProperty("Mode", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        var value = prop?.GetValue(instance);
        if (value is null)
        {
            return StringComparisonModeValue.Default;
        }

        var name = value.ToString();
        return name switch
        {
            "Sensitive" => StringComparisonModeValue.Sensitive,
            "Insensitive" => StringComparisonModeValue.Insensitive,
            _ => StringComparisonModeValue.Default
        };
    }

    private static (string Clause, object Pattern) ResolveStringLike(string column, string paramName, object? value, StringComparisonModeValue mode, bool wrapBoth, bool startsWith, bool endsWith)
    {
        var op = mode == StringComparisonModeValue.Sensitive ? "LIKE" : "ILIKE";
        var raw = value?.ToString() ?? string.Empty;
        var prefix = wrapBoth || endsWith ? "%" : string.Empty;
        var suffix = wrapBoth || startsWith ? "%" : string.Empty;
        var pattern = $"{prefix}{raw}{suffix}";
        return ($"{column} {op} {paramName}", pattern);
    }

    private static (string Clause, object Parameter) BuildStringComparisonFragments(string column, string paramName, object? value, StringComparisonModeValue mode)
    {
        if (mode == StringComparisonModeValue.Sensitive)
        {
            return ($"{column} = {paramName}", value ?? string.Empty);
        }

        return ($"{column} ILIKE {paramName}", value ?? string.Empty);
    }

    private static string WrapArrayPayload(object? value)
    {
        var element = ExtractJsonElement(value);
        var raw = element.ValueKind == JsonValueKind.Undefined ? "null" : element.GetRawText();
        return "[" + raw + "]";
    }

    private static string BuildArrayPayload(System.Collections.IEnumerable values)
    {
        var parts = new List<string>();
        foreach (var val in values)
        {
            var element = ExtractJsonElement(val);
            var raw = element.ValueKind == JsonValueKind.Undefined ? "null" : element.GetRawText();
            parts.Add(raw);
        }

        return "[" + string.Join(",", parts) + "]";
    }

    private string BuildRelationFilterClause(ModelMetadata parentMeta, FieldMetadata relationField, object filterValue, List<NpgsqlParameter> parameters, ParameterContext paramCtx, string parentAlias, ref AliasContext aliasCtx)
    {
        var childMeta = GetModelMetadata(relationField.ClrType);
        var mapping = ResolveRelationMapping(parentMeta, childMeta, relationField);
        var childAlias = aliasCtx.NextRelationAlias();

        var filterType = filterValue.GetType();
        var some = filterType.GetProperty("Some")?.GetValue(filterValue);
        var none = filterType.GetProperty("None")?.GetValue(filterValue);
        var every = filterType.GetProperty("Every")?.GetValue(filterValue);
        var isProp = filterType.GetProperty("Is")?.GetValue(filterValue);
        var isNot = filterType.GetProperty("IsNot")?.GetValue(filterValue);

        if (relationField.IsList)
        {
            if (some is not null)
            {
                var childWhere = BuildWhere(childMeta, some, parameters, paramCtx, childAlias, ref aliasCtx);
                return BuildExistsClause(childMeta, mapping, parentAlias, childAlias, childWhere, negate: false);
            }
            if (none is not null)
            {
                var childWhere = BuildWhere(childMeta, none, parameters, paramCtx, childAlias, ref aliasCtx);
                return BuildExistsClause(childMeta, mapping, parentAlias, childAlias, childWhere, negate: true);
            }
            if (every is not null)
            {
                var childWhere = BuildWhere(childMeta, every, parameters, paramCtx, childAlias, ref aliasCtx);
                var negated = string.IsNullOrWhiteSpace(childWhere) ? "TRUE" : $"NOT ({childWhere})";
                return BuildExistsClause(childMeta, mapping, parentAlias, childAlias, negated, negate: true, rawCondition: true);
            }
            return string.Empty;
        }

        if (isProp is not null)
        {
            var childWhere = BuildWhere(childMeta, isProp, parameters, paramCtx, childAlias, ref aliasCtx);
            return BuildExistsClause(childMeta, mapping, parentAlias, childAlias, childWhere, negate: false);
        }
        if (isNot is not null)
        {
            var childWhere = BuildWhere(childMeta, isNot, parameters, paramCtx, childAlias, ref aliasCtx);
            return BuildExistsClause(childMeta, mapping, parentAlias, childAlias, childWhere, negate: true);
        }

        return string.Empty;
    }

    private static string BuildExistsClause(ModelMetadata childMeta, RelationMapping mapping, string parentAlias, string childAlias, string childWhere, bool negate, bool rawCondition = false)
    {
        var joinCondition = BuildJoinCondition(mapping, parentAlias, childAlias);
        var condition = string.IsNullOrWhiteSpace(childWhere)
            ? joinCondition
            : rawCondition ? $"{joinCondition} AND {childWhere}" : $"{joinCondition} AND ({childWhere})";

        var subquery = $"SELECT 1 FROM {QuoteIdentifier(childMeta.Name)} AS \"{childAlias}\" WHERE {condition}";
        return negate ? $"NOT EXISTS ({subquery})" : $"EXISTS ({subquery})";
    }

    private static string BuildJoinCondition(RelationMapping mapping, string parentAlias, string childAlias)
    {
        var comparisons = mapping.ParentColumns.Zip(mapping.ChildColumns, (p, c) => $"\"{parentAlias}\".{QuoteIdentifier(p)} = \"{childAlias}\".{QuoteIdentifier(c)}");
        return string.Join(" AND ", comparisons);
    }

    private static Dictionary<string, object?> ExtractPrimaryKeyValues(ModelMetadata meta, object instance)
    {
        if (meta.PrimaryKey is null)
        {
            throw new InvalidOperationException($"Model '{meta.Name}' is missing primary key metadata required for nested writes.");
        }

        var values = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var field in meta.PrimaryKey.Fields)
        {
            var prop = instance.GetType().GetProperty(field, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)
                       ?? throw new InvalidOperationException($"Property '{field}' not found on type '{instance.GetType().Name}'.");
            var value = prop.GetValue(instance);
            if (value is null)
            {
                throw new InvalidOperationException($"Primary key field '{field}' on '{meta.Name}' was null after mutation; nested writes require concrete keys.");
            }
            values[field] = value;
        }

        return values;
    }

    private static ForeignKeyMetadata ResolveChildForeignKey(ModelMetadata parentMeta, ModelMetadata childMeta, FieldMetadata relationField)
    {
        var matches = childMeta.ForeignKeys
            .Where(fk => string.Equals(fk.PrincipalModel, parentMeta.Name, StringComparison.Ordinal)
                         && (fk.RelationName is null || string.Equals(fk.RelationName, relationField.Name, StringComparison.Ordinal)))
            .ToList();

        if (matches.Count == 0)
        {
            throw new NotSupportedException($"Relation '{relationField.Name}' from '{parentMeta.Name}' is not backed by a child-owned foreign key.");
        }

        if (matches.Count > 1)
        {
            throw new NotSupportedException($"Relation '{relationField.Name}' from '{parentMeta.Name}' maps to multiple foreign keys on '{childMeta.Name}'. Please disambiguate the relation name.");
        }

        return matches[0];
    }

    private static void AppendFkPredicate(ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk, string alias, ParameterContext paramCtx, List<NpgsqlParameter> parameters, List<string> predicates)
    {
        for (int i = 0; i < fk.LocalFields.Count; i++)
        {
            var principalField = fk.PrincipalFields[i];
            if (!parentPk.TryGetValue(principalField, out var value))
            {
                throw new InvalidOperationException($"Parent primary key field '{principalField}' was not found while building nested write predicates.");
            }

            var paramName = paramCtx.Next();
            predicates.Add($"\"{alias}\".{QuoteIdentifier(fk.LocalFields[i])} = {paramName}");
            parameters.Add(CreateParameter(paramName, GetParameterValue(value)));
        }
    }

    private static void AppendFkAssignments(ForeignKeyMetadata fk, IReadOnlyDictionary<string, object?> parentPk, ParameterContext paramCtx, List<NpgsqlParameter> parameters, List<string> assignments, bool setNull = false)
    {
        for (int i = 0; i < fk.LocalFields.Count; i++)
        {
            var principalField = fk.PrincipalFields[i];
            if (!parentPk.TryGetValue(principalField, out var value))
            {
                throw new InvalidOperationException($"Parent primary key field '{principalField}' was not found while building FK assignments.");
            }

            var paramName = paramCtx.Next();
            assignments.Add($"{QuoteIdentifier(fk.LocalFields[i])} = {paramName}");
            parameters.Add(CreateParameter(paramName, GetParameterValue(setNull ? null : value)));
        }
    }

    private static bool FkAllowsNull(ForeignKeyMetadata fk, ModelMetadata childMeta)
    {
        var nullableColumns = childMeta.Fields
            .Where(f => f.Kind == FieldKind.Scalar && fk.LocalFields.Contains(f.Name, StringComparer.Ordinal))
            .ToDictionary(f => f.Name, f => f.IsNullable, StringComparer.Ordinal);

        return fk.LocalFields.All(col => nullableColumns.TryGetValue(col, out var isNullable) && isNullable);
    }

    private static (string Sql, List<NpgsqlParameter> Parameters, ParameterContext ParamCtx) BuildUpdateSet(ModelMetadata meta, object data, ParameterContext? ctx = null, bool allowEmpty = false)
    {
        var parameters = new List<NpgsqlParameter>();
        var paramCtx = ctx ?? new ParameterContext();
        var assignments = new List<string>();
        var updatedAtField = meta.Fields.FirstOrDefault(f => f.Kind == FieldKind.Scalar && f.IsUpdatedAt);
        var explicitUpdatedAt = false;

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var prop = data.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            if (prop is null)
            {
                continue;
            }

            var value = prop.GetValue(data);
            var isUnset = IsUnsetValue(value, prop.PropertyType);

            if (field.IsUpdatedAt)
            {
                explicitUpdatedAt = !isUnset;
                continue;
            }

            if (isUnset)
            {
                continue;
            }

            var paramName = paramCtx.Next();
            assignments.Add($"{QuoteIdentifier(field.Name)} = {paramName}");
            parameters.Add(CreateParameter(paramName, value, field.ClrType));
        }

        if (updatedAtField is not null && (assignments.Count > 0 || explicitUpdatedAt))
        {
            var prop = data.GetType().GetProperty(updatedAtField.Name, BindingFlags.Public | BindingFlags.Instance);
            object? value = prop?.GetValue(data);
            var isUnset = IsUnsetValue(value, prop?.PropertyType ?? typeof(DateTime));
            if (isUnset)
            {
                value = DateTime.UtcNow;
                if (prop is not null && prop.CanWrite)
                {
                    prop.SetValue(data, value);
                }
            }

            var paramName = paramCtx.Next();
            assignments.Add($"{QuoteIdentifier(updatedAtField.Name)} = {paramName}");
            parameters.Add(CreateParameter(paramName, value, updatedAtField.ClrType));
        }

        if (assignments.Count == 0)
        {
            if (!allowEmpty)
            {
                throw new InvalidOperationException("No scalar fields were provided for update.");
            }

            return (string.Empty, parameters, paramCtx);
        }

        return (string.Join(", ", assignments), parameters, paramCtx);
    }

    private static (IReadOnlyList<string> Columns, List<NpgsqlParameter> Parameters) BuildInsertColumns(ModelMetadata meta, object data, ParameterContext? ctx = null, Dictionary<string, object?>? overrideScalars = null)
    {
        var columns = new List<string>();
        var parameters = new List<NpgsqlParameter>();
        var paramCtx = ctx ?? new ParameterContext();

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var prop = data.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            if (prop is null)
            {
                continue;
            }

            var value = prop.GetValue(data);
            var isUnset = IsUnsetValue(value, prop.PropertyType);

            if (isUnset)
            {
                // defaults first
                if (field.DefaultValue is not null)
                {
                    switch (field.DefaultValue.Kind)
                    {
                        case DefaultValueKind.Autoincrement:
                            continue; // rely on DB DEFAULT
                        case DefaultValueKind.UuidV4:
                            value = Guid.NewGuid();
                            break;
                        case DefaultValueKind.UuidV7:
                            value = GenerateGuidV7();
                            break;
                        case DefaultValueKind.Now:
                            value = DateTime.UtcNow;
                            break;
                        case DefaultValueKind.Json:
                            value = ParseJsonDefault(field.DefaultValue.Value);
                            break;
                        case DefaultValueKind.Static:
                            value = ConvertStaticDefault(field.DefaultValue.Value, prop.PropertyType);
                            break;
                    }

                    if (prop.CanWrite)
                    {
                        prop.SetValue(data, value);
                    }
                    isUnset = IsUnsetValue(value, prop.PropertyType);
                }

                if (isUnset && field.IsPrimaryKey)
                {
                    var generated = GenerateValueForUnsetPrimaryKey(prop.PropertyType);
                    if (generated is not null)
                    {
                        value = generated;
                        if (prop.CanWrite)
                        {
                            prop.SetValue(data, generated);
                        }
                        isUnset = false;
                    }
                }

                if (isUnset && field.IsUpdatedAt)
                {
                    value = DateTime.UtcNow;
                    if (prop.CanWrite)
                    {
                        prop.SetValue(data, value);
                    }
                }
            }

            var paramName = paramCtx.Next();
            columns.Add(field.Name);
            parameters.Add(CreateParameter(paramName, value, field.ClrType));
        }

        if (overrideScalars is not null)
        {
            foreach (var kvp in overrideScalars)
            {
                var existingIndex = columns.FindIndex(c => string.Equals(c, kvp.Key, StringComparison.Ordinal));
                if (existingIndex >= 0)
                {
                    var existingParamName = parameters[existingIndex].ParameterName;
                    parameters[existingIndex] = CreateParameter(existingParamName, kvp.Value);

                    var prop = data.GetType().GetProperty(kvp.Key, BindingFlags.Public | BindingFlags.Instance);
                    if (prop is not null && prop.CanWrite)
                    {
                        prop.SetValue(data, kvp.Value);
                    }

                    continue;
                }

                var paramName = paramCtx.Next();
                columns.Add(kvp.Key);
                parameters.Add(CreateParameter(paramName, kvp.Value));
            }
        }

        return (columns, parameters);
    }

    private string BuildOrderBy(ModelMetadata meta, System.Collections.IEnumerable? orderBy, string tableAlias, ref AliasContext aliasCtx)
    {
        if (orderBy is null)
        {
            return string.Empty;
        }

        var terms = new List<OrderTerm>();
        foreach (var item in orderBy)
        {
            terms.AddRange(BuildOrderTerms(meta, item, tableAlias, ref aliasCtx));
        }

        AppendStableTieBreaker(meta, tableAlias, terms);

        return string.Join(", ", terms.Select(t => $"{t.Expression} {t.Direction}"));
    }

    private static void AppendStableTieBreaker(ModelMetadata meta, string tableAlias, List<OrderTerm> terms)
    {
        if (meta.PrimaryKey is null || meta.PrimaryKey.Fields.Count == 0)
        {
            return;
        }

        var existing = new HashSet<string>(terms.Select(t => t.Expression), StringComparer.Ordinal);
        foreach (var pkField in meta.PrimaryKey.Fields)
        {
            var expr = $"\"{tableAlias}\".{QuoteIdentifier(pkField)}";
            if (existing.Contains(expr))
            {
                continue;
            }

            terms.Add(new OrderTerm(expr, "ASC"));
        }
    }

    private List<OrderTerm> BuildOrderTerms(ModelMetadata meta, object orderObj, string tableAlias, ref AliasContext aliasCtx)
    {
        var terms = new List<OrderTerm>();
        var type = orderObj.GetType();

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var prop = type.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            var value = prop?.GetValue(orderObj);
            if (value is null)
            {
                continue;
            }

            var direction = ResolveSortDirection(value);
            terms.Add(new OrderTerm($"\"{tableAlias}\".{QuoteIdentifier(field.Name)}", direction));
        }

        foreach (var relation in meta.Fields.Where(f => f.Kind == FieldKind.Relation))
        {
            var prop = type.GetProperty(relation.Name, BindingFlags.Public | BindingFlags.Instance);
            var value = prop?.GetValue(orderObj);
            if (value is null)
            {
                continue;
            }

            terms.AddRange(BuildRelationOrderTerms(meta, relation, value, tableAlias, ref aliasCtx));
        }

        return terms;
    }

    private List<OrderTerm> BuildRelationOrderTerms(ModelMetadata parentMeta, FieldMetadata relationField, object orderValue, string parentAlias, ref AliasContext aliasCtx)
    {
        var childMeta = GetModelMetadata(relationField.ClrType);
        var mapping = ResolveRelationMapping(parentMeta, childMeta, relationField);
        var childAlias = aliasCtx.NextOrderAlias();

        var childTerms = BuildOrderTerms(childMeta, orderValue, childAlias, ref aliasCtx);
        if (childTerms.Count == 0)
        {
            return childTerms;
        }

        var joinCondition = BuildJoinCondition(mapping, parentAlias, childAlias);
        var orderList = string.Join(", ", childTerms.Select(t => $"{t.Expression} {t.Direction}"));
        var projectedTerms = new List<OrderTerm>(childTerms.Count);

        foreach (var term in childTerms)
        {
            var subquery = $"(SELECT {term.Expression} FROM {QuoteIdentifier(childMeta.Name)} AS \"{childAlias}\" WHERE {joinCondition} ORDER BY {orderList} LIMIT 1)";
            projectedTerms.Add(new OrderTerm(subquery, term.Direction));
        }

        return projectedTerms;
    }

    private static string ResolveSortDirection(object value)
    {
        var order = value.ToString();
        return string.Equals(order, "Desc", StringComparison.OrdinalIgnoreCase) ? "DESC" : "ASC";
    }

    private static string BuildTypeMismatchMessage(Type expected, Type actual, QueryModel query)
    {
        return $"Expected result of type '{expected.FullName}' for {query.Type} on model '{query.ModelName}', but received '{actual.FullName}'.";
    }

    private static string BuildNullResultMessage(Type expected, QueryModel query)
    {
        return $"Expected result of type '{expected.FullName}' for {query.Type} on model '{query.ModelName}', but received null.";
    }

    private static IReadOnlyList<T> CastToReadOnlyList<T>(object? result, QueryModel query)
    {
        if (result is IReadOnlyList<T> readOnly)
        {
            return readOnly;
        }

        if (result is IEnumerable<T> enumerable)
        {
            return enumerable.ToList();
        }

        if (result is System.Collections.IEnumerable nonGeneric)
        {
            var list = new List<T>();
            foreach (var item in nonGeneric)
            {
                if (item is not T casted)
                {
                    throw new InvalidOperationException(BuildTypeMismatchMessage(typeof(IReadOnlyList<T>), result.GetType(), query));
                }
                list.Add(casted);
            }
            return list;
        }

        throw new InvalidOperationException(result is null
            ? BuildNullResultMessage(typeof(IReadOnlyList<T>), query)
            : BuildTypeMismatchMessage(typeof(IReadOnlyList<T>), result.GetType(), query));
    }

    private static void MaterializeRow(ModelMetadata meta, IDataRecord reader, object instance)
    {
        var type = instance.GetType();
        for (int i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            var prop = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (prop is null || !prop.CanWrite)
            {
                continue;
            }

            SetScalarProperty(prop, instance, reader, i);
        }
    }

    private static void MaterializeInto(ModelMetadata meta, IDataRecord reader, object instance, string prefix)
    {
        var type = instance.GetType();
        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            var columnName = string.IsNullOrEmpty(prefix) ? field.Name : $"{prefix}{field.Name}";
            var ordinal = TryGetOrdinal(reader, columnName);
            if (ordinal < 0)
            {
                continue;
            }

            var prop = type.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (prop is null || !prop.CanWrite)
            {
                continue;
            }

            SetScalarProperty(prop, instance, reader, ordinal);
        }
    }

    private static void SetScalarProperty(PropertyInfo prop, object instance, IDataRecord reader, int ordinal)
    {
        var value = reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal);
        if (value is null)
        {
            prop.SetValue(instance, null);
            return;
        }

        var targetType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
        if (IsJsonWrapperType(targetType))
        {
            var element = ExtractJsonElement(value);
            var wrapper = CreateJsonWrapper(targetType, element.Clone());
            prop.SetValue(instance, wrapper);
        }
        else if (targetType == typeof(JsonElement))
        {
            if (value is string s)
            {
                prop.SetValue(instance, JsonDocument.Parse(s).RootElement);
            }
            else if (value is JsonDocument doc)
            {
                prop.SetValue(instance, doc.RootElement);
            }
            else
            {
                prop.SetValue(instance, JsonSerializer.Deserialize<JsonElement>(JsonSerializer.Serialize(value)));
            }
        }
        else if (targetType == typeof(byte[]))
        {
            prop.SetValue(instance, value);
        }
        else if (targetType == typeof(Guid))
        {
            prop.SetValue(instance, value is Guid g ? g : Guid.Parse(value.ToString()!));
        }
        else if (targetType.IsEnum)
        {
            prop.SetValue(instance, Enum.Parse(targetType, value.ToString()!, ignoreCase: true));
        }
        else
        {
            prop.SetValue(instance, Convert.ChangeType(value, targetType));
        }
    }

    private static bool IsSimpleValue(object value)
    {
        return value is string || value.GetType().IsValueType || value is Guid;
    }

    private static bool IsJsonClrType(string? clrType)
    {
        return clrType is not null && (clrType.Equals("Json", StringComparison.Ordinal) || clrType.Equals("JsonElement", StringComparison.Ordinal));
    }

    private static bool IsStringClrType(string? clrType)
    {
        return clrType is not null && clrType.Equals("string", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsJsonWrapperType(Type targetType)
    {
        return IsJsonClrType(targetType.Name) || targetType.FullName?.EndsWith(".Json.Json", StringComparison.Ordinal) == true;
    }

    private static bool IsJsonValue(object? value)
    {
        if (value is null)
        {
            return false;
        }

        return value is JsonElement || IsJsonWrapperType(value.GetType());
    }

    private static JsonElement ExtractJsonElement(object? value)
    {
        if (value is null)
        {
            return default;
        }

        if (value is JsonElement element)
        {
            return element;
        }

        var type = value.GetType();
        var elementProp = type.GetProperty("Element") ?? type.GetProperty("Value") ?? type.GetProperty("Root");
        if (elementProp?.GetValue(value) is JsonElement propElement)
        {
            return propElement;
        }

        var rawProp = type.GetProperty("RawText") ?? type.GetProperty("GetRawText") ?? type.GetProperty("Text") ?? null;
        if (rawProp?.GetValue(value) is string raw)
        {
            return JsonDocument.Parse(raw).RootElement;
        }

        return JsonSerializer.SerializeToElement(value);
    }

    private static string? ExtractJsonRawText(object? value)
    {
        if (value is null)
        {
            return null;
        }

        if (value is string s)
        {
            return s;
        }

        if (value is JsonElement element)
        {
            return element.GetRawText();
        }

        var type = value.GetType();
        var rawMethod = type.GetMethod("GetRawText", BindingFlags.Public | BindingFlags.Instance);
        if (rawMethod is not null)
        {
            var rawVal = rawMethod.Invoke(value, Array.Empty<object?>());
            if (rawVal is string rawText)
            {
                return rawText;
            }
        }

        var rawProp = type.GetProperty("GetRawText") ?? type.GetProperty("RawText");
        if (rawProp is not null)
        {
            var rawVal = rawProp.GetValue(value);
            if (rawVal is string rawText)
            {
                return rawText;
            }
        }

        if (type.FullName?.EndsWith(".Json.Json", StringComparison.Ordinal) == true)
        {
            var elementProp = type.GetProperty("Element") ?? type.GetProperty("Value") ?? type.GetProperty("Root");
            if (elementProp?.GetValue(value) is JsonElement propElement)
            {
                return propElement.GetRawText();
            }
        }

        return JsonSerializer.Serialize(value);
    }

    private static object CreateJsonWrapper(Type targetType, JsonElement element)
    {
        var ctor = targetType.GetConstructor(new[] { typeof(JsonElement) });
        if (ctor is not null)
        {
            return ctor.Invoke(new object[] { element });
        }

        var parse = targetType.GetMethod("FromElement", BindingFlags.Public | BindingFlags.Static, new[] { typeof(JsonElement) })
                    ?? targetType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, new[] { typeof(string) });

        if (parse is not null)
        {
            return parse.GetParameters()[0].ParameterType == typeof(JsonElement)
                ? parse.Invoke(null, new object[] { element })!
                : parse.Invoke(null, new object[] { element.GetRawText() })!;
        }

        return element;
    }

    private static object? GetParameterValue(object? value)
    {
        if (value is Enum enumValue)
        {
            return enumValue.ToString();
        }

        return value ?? DBNull.Value;
    }

    private static NpgsqlParameter CreateParameter(string name, object? value, string? clrType = null)
    {
        // JSON requires explicit typing and serialization for pg jsonb columns.
        if (IsJsonClrType(clrType) || IsJsonValue(value))
        {
            var raw = ExtractJsonRawText(value);
            var jsonParam = new NpgsqlParameter(name, NpgsqlDbType.Jsonb)
            {
                Value = raw ?? (object)DBNull.Value
            };
            return jsonParam;
        }

        var param = new NpgsqlParameter(name, GetParameterValue(value));

        if (clrType is not null && IsEnumClrType(clrType))
        {
            param.DataTypeName = ToPgEnumTypeName(clrType);
        }

        return param;
    }

    private static bool IsEnumClrType(string clrType)
    {
        return clrType switch
        {
            "string" or "bool" or "byte" or "short" or "int" or "long" or "float" or "double" or "decimal" or "Guid" or "DateTime" or "JsonElement" or "Json" or "byte[]" => false,
            _ => true
        };
    }

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

    private static void EnsureIncludeNotProvided(object? include)
    {
        if (include is not null)
        {
            throw new NotSupportedException("Include projections are not supported by PostgresSqlExecutor yet.");
        }
    }

    private static void EnsureNoRelationWrites(ModelMetadata meta, object data)
    {
        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Relation))
        {
            var prop = data.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
            if (prop is null)
            {
                continue;
            }

            if (prop.GetValue(data) is not null)
            {
                throw new NotSupportedException($"Nested writes for relation '{field.Name}' are not supported in this executor.");
            }
        }
    }

    private static IReadOnlyDictionary<string, ModelMetadata> LoadMetadata(string rootNamespace, Assembly assembly)
    {
        var registryType = assembly.GetType($"{rootNamespace}.Metadata.ModelMetadataRegistry");
        if (registryType is null)
        {
            throw new InvalidOperationException($"Could not find ModelMetadataRegistry in namespace '{rootNamespace}'.");
        }
        var prop = registryType.GetProperty("All", BindingFlags.Public | BindingFlags.Static);
        if (prop?.GetValue(null) is not IReadOnlyDictionary<string, ModelMetadata> registry)
        {
            throw new InvalidOperationException("ModelMetadataRegistry.All not found or invalid.");
        }
        return registry;
    }

    private static string QuoteIdentifier(string identifier)
    {
        var folded = _preserveIdentifierCasing ? identifier : identifier.ToLowerInvariant();
        return $"\"{folded}\"";
    }

    private sealed record RelationMapping(IReadOnlyList<string> ParentColumns, IReadOnlyList<string> ChildColumns);

    private sealed record RelationForeignKey(ForeignKeyOwner Owner, ForeignKeyMetadata ForeignKey);

    private enum ForeignKeyOwner
    {
        Child,
        Parent
    }

    private sealed record OrderTerm(string Expression, string Direction);

    private sealed class ParameterContext
    {
        private int _counter;

        public string Next()
        {
            var id = Interlocked.Increment(ref _counter);
            return $"@p{id}";
        }
    }

    private sealed class AliasContext
    {
        private int _relationCounter;
        private int _orderCounter;

        public string NextRelationAlias()
        {
            var id = Interlocked.Increment(ref _relationCounter);
            return $"rf{id}";
        }

        public string NextOrderAlias()
        {
            var id = Interlocked.Increment(ref _orderCounter);
            return $"ob{id}";
        }
    }
}
