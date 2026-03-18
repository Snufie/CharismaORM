using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Charisma.QueryEngine.Planning;
using Npgsql;

namespace Charisma.QueryEngine.Execution;

public sealed partial class PostgresSqlExecutor
{
    private Task<object?> ExecuteQueryInternalAsync(QueryModel model, SqlExecutionContext? context, CancellationToken ct)
    {
        return model.Type switch
        {
            QueryType.FindUnique => ExecutePlannedQueryAsync(model, SqlPlanKind.QuerySingle, context, ct),
            QueryType.FindFirst => ExecuteFindFirstAsync(model, context, ct),
            QueryType.FindMany => ExecutePlannedQueryAsync(model, SqlPlanKind.QueryMany, context, ct),
            QueryType.Create => ExecuteCreateAsync(model, context, ct),
            QueryType.CreateMany => ExecuteCreateManyAsync(model, context, ct),
            QueryType.Update => ExecuteUpdateAsync(model, context, ct),
            QueryType.UpdateMany => ExecuteUpdateManyAsync(model, context, ct),
            QueryType.Upsert => ExecuteUpsertAsync(model, context, ct),
            QueryType.Delete => ExecuteDeleteAsync(model, context, ct),
            QueryType.Count => ExecuteCountAsync(model, context, ct),
            QueryType.Aggregate => ExecuteAggregateAsync(model, context, ct),
            QueryType.GroupBy => ExecuteGroupByAsync(model, context, ct),
            _ => throw new NotSupportedException($"Query type '{model.Type}' is not supported by PostgresSqlExecutor ExecuteQueryAsync.")
        };
    }

    private Task<int> ExecuteCommandInternalAsync(QueryModel model, SqlExecutionContext? context, CancellationToken ct)
    {
        return model.Type switch
        {
            QueryType.CreateMany when model is CreateManyQueryModel { ReturnRecords: false } => ExecuteCreateManyNonQueryAsync(model, context, ct),
            QueryType.UpdateMany when model is UpdateManyQueryModel { ReturnRecords: false } => ExecuteUpdateManyNonQueryAsync(model, context, ct),
            QueryType.DeleteMany => ExecuteDeleteManyNonQueryAsync(model, context, ct),
            _ => throw new NotSupportedException($"Query type '{model.Type}' is not supported by PostgresSqlExecutor ExecuteCommandAsync.")
        };
    }

    private async Task<object?> ExecutePlannedQueryAsync(QueryModel query, SqlPlanKind expectedKind, SqlExecutionContext? context, CancellationToken ct)
    {
        var plan = _planner.Plan(query);
        EnsurePlanKind(plan, expectedKind);

        var (conn, tx) = ExtractContext(context);
        var meta = plan.Model ?? GetModelMetadata(query.ModelName);
        var includes = plan.IncludeRoot?.Children ?? Array.Empty<IncludePlan>();
        var parameters = plan.Parameters.Select(p => new NpgsqlParameter(p.Name, p.Value ?? DBNull.Value)).ToList();

        var logEnv = Environment.GetEnvironmentVariable("CHARISMA_LOG_SQL");
        var logSql = string.Equals(logEnv?.Trim(), "1", StringComparison.Ordinal)
            || string.Equals(logEnv?.Trim(), "true", StringComparison.OrdinalIgnoreCase);
        if (logSql)
        {
            Console.WriteLine($"[SQL {query.Type}] {plan.CommandText}");
            if (parameters.Count > 0)
            {
                foreach (var p in parameters)
                {
                    Console.WriteLine($"  {p.ParameterName} = {p.Value ?? "<null>"}");
                }
            }
        }

        switch (plan.Kind)
        {
            case SqlPlanKind.QuerySingle:
                {
                    var results = includes.Count == 0
                        ? conn is null
                            ? await ExecuteReaderAsync(meta, plan.CommandText, parameters, ct, query.Type.ToString()).ConfigureAwait(false)
                            : await ExecuteReaderAsync(meta, plan.CommandText, parameters, conn, tx, ct, query.Type.ToString()).ConfigureAwait(false)
                        : await ExecuteReaderWithIncludesAsync(meta, includes, plan.CommandText, parameters, ct, query.Type.ToString(), conn, tx).ConfigureAwait(false);
                    var materialized = ApplyDistinctAndPaging(meta, plan, results);
                    return materialized.Count == 0 ? null : materialized[0];
                }
            case SqlPlanKind.QueryMany:
                {
                    var results = includes.Count == 0
                        ? conn is null
                            ? await ExecuteReaderAsync(meta, plan.CommandText, parameters, ct, query.Type.ToString()).ConfigureAwait(false)
                            : await ExecuteReaderAsync(meta, plan.CommandText, parameters, conn, tx, ct, query.Type.ToString()).ConfigureAwait(false)
                        : await ExecuteReaderWithIncludesAsync(meta, includes, plan.CommandText, parameters, ct, query.Type.ToString(), conn, tx).ConfigureAwait(false);
                    return ApplyDistinctAndPaging(meta, plan, results);
                }
            default:
                throw new NotSupportedException($"Plan kind '{plan.Kind}' is not supported in ExecutePlannedQueryAsync.");
        }
    }

    private async Task<object?> ExecuteFindFirstAsync(QueryModel model, SqlExecutionContext? context, CancellationToken ct)
    {
        var result = await ExecutePlannedQueryAsync(model, SqlPlanKind.QuerySingle, context, ct).ConfigureAwait(false);
        if (result is null && model is FindFirstQueryModel ff && ff.ThrowIfNotFound)
        {
            throw new RecordNotFoundException(model.ModelName, nameof(QueryType.FindFirst));
        }
        return result;
    }

    private async Task<object?> ExecuteCountAsync(QueryModel model, SqlExecutionContext? context, CancellationToken ct)
    {
        var plan = _planner.Plan(model);
        var (conn, tx) = ExtractContext(context);
        var parameters = plan.Parameters.Select(p => new NpgsqlParameter(p.Name, p.Value ?? DBNull.Value)).ToList();
        if (plan.Kind == SqlPlanKind.QuerySingle)
        {
            var count = await ExecuteScalarAsync(plan.CommandText, parameters, conn, tx, ct, model.ModelName, model.Type.ToString()).ConfigureAwait(false);
            return count;
        }

        if (plan.Kind == SqlPlanKind.QueryMany)
        {
            var meta = plan.Model ?? GetModelMetadata(model.ModelName);
            var rows = await ExecuteReaderAsync(meta, plan.CommandText, parameters, ct, model.Type.ToString()).ConfigureAwait(false);
            var distinctRows = ApplyDistinctAndPaging(meta, plan, rows);
            return distinctRows.Count;
        }

        throw new InvalidOperationException($"Planner returned kind '{plan.Kind}' but executor expected '{SqlPlanKind.QuerySingle}' or '{SqlPlanKind.QueryMany}'.");
    }

    private async Task<object?> ExecuteAggregateAsync(QueryModel model, SqlExecutionContext? context, CancellationToken ct)
    {
        var plan = _planner.Plan(model);
        var (conn, tx) = ExtractContext(context);
        var parameters = plan.Parameters.Select(p => new NpgsqlParameter(p.Name, p.Value ?? DBNull.Value)).ToList();

        if (plan.Kind == SqlPlanKind.QuerySingle)
        {
            var resultType = ResolveAggregateResultType(model.ModelName);
            return await ExecuteAggregateInternalAsync(resultType, plan.CommandText, parameters, conn, tx, ct, model.Type.ToString()).ConfigureAwait(false);
        }

        if (plan.Kind == SqlPlanKind.QueryMany)
        {
            var meta = plan.Model ?? GetModelMetadata(model.ModelName);
            var rows = await ExecuteReaderAsync(meta, plan.CommandText, parameters, ct, model.Type.ToString()).ConfigureAwait(false);
            var distinctRows = ApplyDistinctAndPaging(meta, plan, rows);
            return ComputeAggregateResult(meta, model, distinctRows);
        }

        throw new InvalidOperationException($"Planner returned kind '{plan.Kind}' but executor expected '{SqlPlanKind.QuerySingle}' or '{SqlPlanKind.QueryMany}'.");
    }

    private async Task<object?> ExecuteGroupByAsync(QueryModel model, SqlExecutionContext? context, CancellationToken ct)
    {
        try
        {
            var plan = _planner.Plan(model);
            if (plan.Kind != SqlPlanKind.QueryMany)
            {
                throw new InvalidOperationException($"Planner returned kind '{plan.Kind}' but executor expected '{SqlPlanKind.QueryMany}'.");
            }

            var (conn, tx) = ExtractContext(context);
            var parameters = plan.Parameters.Select(p => new NpgsqlParameter(p.Name, p.Value ?? DBNull.Value)).ToList();
            var meta = GetModelMetadata(model.ModelName);
            var resultType = ResolveGroupByResultType(model.ModelName);

            var ownsConnection = conn is null;
            await using var ownedConn = ownsConnection ? await _connectionProvider.OpenAsync(ct).ConfigureAwait(false) : null;
            var npgConn = conn ?? ownedConn as NpgsqlConnection;
            if (npgConn is null)
            {
                throw new InvalidOperationException("ConnectionProvider returned a non-Npgsql connection.");
            }

            var results = new List<object>();
            await using var cmd = new NpgsqlCommand(plan.CommandText, npgConn)
            {
                Transaction = tx
            };
            foreach (var p in parameters)
            {
                cmd.Parameters.Add(p);
            }

            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var instance = Activator.CreateInstance(resultType) ?? throw new InvalidOperationException($"Failed to create groupBy result instance of '{resultType.FullName}'.");
                PopulateGroupByResult(meta, instance, reader);
                results.Add(instance);
            }

            return results.AsReadOnly();
        }
        catch (PostgresException ex)
        {
            throw MapProviderException(ex, model.ModelName, model.Type.ToString());
        }
    }

    private async Task<IReadOnlyList<T>> ExecutePlannedQueryManyTypedAsync<T>(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var plan = _planner.Plan(query);
        EnsurePlanKind(plan, SqlPlanKind.QueryMany);

        var (conn, tx) = ExtractContext(context);
        var meta = plan.Model ?? GetModelMetadata(query.ModelName);
        var includes = plan.IncludeRoot?.Children ?? Array.Empty<IncludePlan>();
        var parameters = plan.Parameters.Select(p => new NpgsqlParameter(p.Name, p.Value ?? DBNull.Value)).ToList();

        var modelType = _modelTypeResolver(meta.Name);
        if (!typeof(T).IsAssignableFrom(modelType))
        {
            throw new InvalidOperationException(BuildTypeMismatchMessage(typeof(T), modelType, query));
        }
        try
        {
            var results = await ExecuteReaderWithIncludesAsync(meta, includes, plan.CommandText, parameters, ct, query.Type.ToString(), conn, tx).ConfigureAwait(false);
            var distinctResults = ApplyDistinctAndPaging(meta, plan, results);
            return CastToReadOnlyList<T>(distinctResults, query);
        }
        catch (PostgresException ex)
        {
            Console.Error.WriteLine($"SQL failed ({query.Type} {query.ModelName}): {plan.CommandText}");
            throw MapProviderException(ex, query.ModelName, query.Type.ToString());
        }
    }

    private static Exception MapProviderException(PostgresException ex, string modelName, string operation)
    {
        return ex.SqlState switch
        {
            PostgresErrorCodes.UniqueViolation => new UniqueConstraintViolationException(modelName, operation, ex.ConstraintName, ex),
            PostgresErrorCodes.ForeignKeyViolation => new ForeignKeyViolationException(modelName, operation, ex.ConstraintName, ex),
            _ => new DatabaseExecutionException(
                modelName,
                operation,
                $"Database error while executing '{operation}' on model '{modelName}' (SQLSTATE {ex.SqlState}). {ex.MessageText}",
                ex.SqlState,
                ex)
        };
    }

    private static void EnsurePlanKind(SqlPlan plan, SqlPlanKind expected)
    {
        if (plan.Kind != expected)
        {
            throw new InvalidOperationException($"Planner returned kind '{plan.Kind}' but executor expected '{expected}'.");
        }
    }

    private IReadOnlyList<object> ApplyDistinctAndPaging(ModelMetadata meta, SqlPlan plan, IReadOnlyList<object> results)
    {
        var hasDistinct = plan.DistinctFields is { Count: > 0 };
        var hasPostPaging = plan.PostDistinctSkip.HasValue || plan.PostDistinctTake.HasValue;
        if (!hasDistinct && !hasPostPaging)
        {
            return results;
        }

        var keyFields = hasDistinct ? plan.DistinctFields! : Array.Empty<string>();
        var seen = new HashSet<CompositeKey>(CompositeKeyComparer.Instance);
        var deduped = new List<object>();

        foreach (var item in results)
        {
            if (hasDistinct)
            {
                var key = new CompositeKey(BuildKeyValues(meta, keyFields, item));
                if (!seen.Add(key))
                {
                    continue;
                }
            }

            deduped.Add(item);
        }

        var skip = plan.PostDistinctSkip ?? 0;
        var take = plan.PostDistinctTake;
        if (skip == 0 && !take.HasValue)
        {
            return deduped;
        }

        var sliced = take.HasValue ? deduped.Skip(skip).Take(take.Value) : deduped.Skip(skip);
        return sliced.ToList();
    }

    private static IReadOnlyList<object?> BuildKeyValues(ModelMetadata meta, IReadOnlyList<string> keyFields, object instance)
    {
        var values = new List<object?>(keyFields.Count);
        var type = instance.GetType();
        foreach (var field in keyFields)
        {
            var prop = type.GetProperty(field, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            values.Add(prop?.GetValue(instance));
        }

        return values;
    }

    private object ComputeAggregateResult(ModelMetadata meta, QueryModel model, IReadOnlyList<object> rows)
    {
        var args = model.Args;
        var aggregate = args.GetType().GetProperty("Aggregate", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)?.GetValue(args)
            ?? throw new InvalidOperationException("Aggregate selectors must be provided via the Aggregate property.");

        var resultType = ResolveAggregateResultType(meta.Name);
        var result = Activator.CreateInstance(resultType) ?? throw new InvalidOperationException($"Failed to create aggregate result type for '{meta.Name}'.");

        if (GetSelectorFlag(aggregate, "Count"))
        {
            resultType.GetProperty("Count", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)?.SetValue(result, rows.Count);
        }

        PopulateAggregateExtremes(meta, rows, aggregate, result, resultType, isMin: true);
        PopulateAggregateExtremes(meta, rows, aggregate, result, resultType, isMin: false);

        return result;
    }

    private static void PopulateAggregateExtremes(ModelMetadata meta, IReadOnlyList<object> rows, object aggregateSelector, object result, Type resultType, bool isMin)
    {
        var selectorName = isMin ? "Min" : "Max";
        var selector = aggregateSelector.GetType().GetProperty(selectorName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)?.GetValue(aggregateSelector);
        if (selector is null)
        {
            return;
        }

        var targetProp = resultType.GetProperty(selectorName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (targetProp is null)
        {
            return;
        }

        var targetInstance = Activator.CreateInstance(targetProp.PropertyType) ?? throw new InvalidOperationException($"Failed to create aggregate {selectorName} result type for '{meta.Name}'.");

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            if (!GetSelectorFlag(selector, field.Name))
            {
                continue;
            }

            var value = ComputeExtreme(rows, field.Name, isMin);
            var fieldProp = targetProp.PropertyType.GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            fieldProp?.SetValue(targetInstance, value);
        }

        targetProp.SetValue(result, targetInstance);
    }

    private static bool GetSelectorFlag(object selector, string name)
    {
        var prop = selector.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        var flag = prop?.GetValue(selector) as bool?;
        return flag == true;
    }

    private static object? ComputeExtreme(IReadOnlyList<object> rows, string fieldName, bool takeMin)
    {
        object? best = null;
        foreach (var row in rows)
        {
            var value = GetFieldValue(row, fieldName);
            if (value is null)
            {
                continue;
            }

            if (best is null)
            {
                best = value;
                continue;
            }

            if (value is IComparable comparable && best is IComparable current)
            {
                var comparison = comparable.CompareTo(current);
                if ((takeMin && comparison < 0) || (!takeMin && comparison > 0))
                {
                    best = value;
                }
            }
        }

        return best;
    }

    private static object? GetFieldValue(object instance, string fieldName)
    {
        var prop = instance.GetType().GetProperty(fieldName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        return prop?.GetValue(instance);
    }

    private sealed record CompositeKey(IReadOnlyList<object?> Values);

    private sealed class CompositeKeyComparer : IEqualityComparer<CompositeKey>
    {
        public static readonly CompositeKeyComparer Instance = new();

        public bool Equals(CompositeKey? x, CompositeKey? y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }
            if (x is null || y is null || x.Values.Count != y.Values.Count)
            {
                return false;
            }

            for (var i = 0; i < x.Values.Count; i++)
            {
                if (!Equals(x.Values[i], y.Values[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public int GetHashCode(CompositeKey obj)
        {
            unchecked
            {
                var hash = 17;
                foreach (var value in obj.Values)
                {
                    hash = (hash * 23) + (value?.GetHashCode() ?? 0);
                }
                return hash;
            }
        }
    }
}
