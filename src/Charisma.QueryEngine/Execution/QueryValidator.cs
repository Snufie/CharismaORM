using System;
using System.Collections;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Validates QueryModel payloads before dispatch to the SQL executor.
/// Ensures required arguments exist, rejects unsupported combinations, and surfaces clear errors early.
/// </summary>
internal static class QueryValidator
{
    public static void Validate(QueryModel query)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (string.IsNullOrWhiteSpace(query.ModelName))
        {
            throw new QueryValidationException(query.ModelName, query.Type, "ModelName is required.");
        }

        switch (query)
        {
            case FindUniqueQueryModel findUnique:
                ValidateFindUnique(findUnique);
                break;
            case FindManyQueryModel findMany:
                ValidateFindMany(findMany);
                break;
            case FindFirstQueryModel findFirst:
                ValidateFindFirst(findFirst);
                break;
            case CreateQueryModel create:
                ValidateCreate(create);
                break;
            case CreateManyQueryModel createMany:
                ValidateCreateMany(createMany);
                break;
            case UpdateQueryModel update:
                ValidateUpdate(update);
                break;
            case UpdateManyQueryModel updateMany:
                ValidateUpdateMany(updateMany);
                break;
            case UpsertQueryModel upsert:
                ValidateUpsert(upsert);
                break;
            case DeleteQueryModel delete:
                ValidateDelete(delete);
                break;
            case DeleteManyQueryModel deleteMany:
                ValidateDeleteMany(deleteMany);
                break;
            case CountQueryModel count:
                ValidateCount(count);
                break;
            case AggregateQueryModel aggregate:
                ValidateAggregate(aggregate);
                break;
            case GroupByQueryModel groupBy:
                ValidateGroupBy(groupBy);
                break;
            default:
                throw new QueryValidationException(query.ModelName, query.Type, $"Unsupported query type '{query.Type}'.");
        }
    }

    private static void ValidateFindUnique(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureProperty(args, "Where", query, required: true);
        EnsureSelectIncludeExclusivity(args, query);
    }

    private static void ValidateFindMany(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureSelectIncludeExclusivity(args, query);
        EnsureNonNegative(args, "Skip", query);
        EnsureDistinctIncludeExclusivity(args, query);
    }

    private static void ValidateFindFirst(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureSelectIncludeExclusivity(args, query);
        EnsureNonNegative(args, "Skip", query);
        EnsureDistinctIncludeExclusivity(args, query);
    }

    private static void ValidateCreate(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureProperty(args, "Data", query, required: true);
        EnsureSelectIncludeExclusivity(args, query);
    }

    private static void ValidateCreateMany(QueryModel query)
    {
        var args = EnsureArgs(query);
        var data = EnsureProperty(args, "Data", query, required: true)
            ?? throw new QueryValidationException(query.ModelName, query.Type, "Property 'Data' is required for operation 'CreateMany'.");
        EnsureSelectIncludeExclusivity(args, query);
        EnsureIncludeAbsent(args, query);
        EnsureEnumerableNotEmpty(data, "Data", query);
    }

    private static void ValidateUpdate(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureProperty(args, "Data", query, required: true);
        EnsureProperty(args, "Where", query, required: true);
        EnsureSelectIncludeExclusivity(args, query);
    }

    private static void ValidateUpdateMany(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureProperty(args, "Data", query, required: true);
        EnsureSelectIncludeExclusivity(args, query);
        EnsureIncludeAbsent(args, query);
        EnsureWhereOrLimit(args, query);
        EnsurePositive(args, "Limit", query);
    }

    private static void ValidateUpsert(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureProperty(args, "Create", query, required: true);
        EnsureProperty(args, "Update", query, required: true);
        EnsureProperty(args, "Where", query, required: true);
        EnsureSelectIncludeExclusivity(args, query);
    }

    private static void ValidateDelete(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureProperty(args, "Where", query, required: true);
        EnsureSelectIncludeExclusivity(args, query);
        EnsureIncludeAbsent(args, query);
    }

    private static void ValidateDeleteMany(QueryModel query)
    {
        var args = EnsureArgs(query);
        EnsureSelectIncludeExclusivity(args, query);
        EnsureIncludeAbsent(args, query);
    }

    private static void ValidateCount(QueryModel query)
    {
        var args = EnsureArgs(query);
        var distinct = EnsureProperty(args, "Distinct", query, required: false);
        if (distinct is not null && distinct is not System.Collections.IEnumerable)
        {
            throw new QueryValidationException(query.ModelName, query.Type, "Distinct must be an enumerable of field names.");
        }
    }

    private static void ValidateAggregate(QueryModel query)
    {
        var args = EnsureArgs(query);
        var distinct = EnsureProperty(args, "Distinct", query, required: false);
        if (distinct is not null && distinct is not IEnumerable)
        {
            throw new QueryValidationException(query.ModelName, query.Type, "Distinct must be an enumerable of field names.");
        }

        EnsureNonNegative(args, "Skip", query);
        EnsurePositive(args, "Take", query);
        EnsureAggregateSelection(args, query);
    }

    private static object EnsureArgs(QueryModel query)
    {
        return query.Args ?? throw new QueryValidationException(query.ModelName, query.Type, "Args payload is required.");
    }

    private static object? EnsureProperty(object target, string name, QueryModel query, bool required)
    {
        var prop = target.GetType().GetProperty(name);
        if (prop is null)
        {
            throw new QueryValidationException(query.ModelName, query.Type, $"Args type '{target.GetType().Name}' does not define property '{name}'.");
        }

        var value = prop.GetValue(target);
        if (required && value is null)
        {
            throw new QueryValidationException(query.ModelName, query.Type, $"Property '{name}' is required for operation '{query.Type}'.");
        }

        return value;
    }

    private static void EnsureSelectIncludeExclusivity(object args, QueryModel query)
    {
        var select = EnsureProperty(args, "Select", query, required: false);
        var include = EnsureProperty(args, "Include", query, required: false);
        var omit = EnsureProperty(args, "Omit", query, required: false);
        var hasSelect = select is not null;
        var hasInclude = include is not null;
        var hasOmit = omit is not null;

        if ((hasSelect && hasInclude) || (hasSelect && hasOmit) || (hasInclude && hasOmit))
        {
            throw new QueryValidationException(query.ModelName, query.Type, "Select, Include, and Omit are mutually exclusive; choose only one.");
        }
    }

    private static void EnsureIncludeAbsent(object args, QueryModel query)
    {
        var include = EnsureProperty(args, "Include", query, required: false);
        if (include is not null)
        {
            throw new QueryValidationException(query.ModelName, query.Type, "Include is not supported for this operation.");
        }
    }

    private static void EnsureDistinctIncludeExclusivity(object args, QueryModel query)
    {
        var include = EnsureProperty(args, "Include", query, required: false);
        var distinct = EnsureProperty(args, "Distinct", query, required: false);
        // Distinct may be combined with include/select/omit; deduplication happens in-memory before projection and pagination.
    }

    private static void EnsureWhereOrLimit(object args, QueryModel query)
    {
        var where = EnsureProperty(args, "Where", query, required: false);
        var limit = EnsureProperty(args, "Limit", query, required: false) as int?;
        if (where is null && (!limit.HasValue || limit.Value <= 0))
        {
            throw new QueryValidationException(query.ModelName, query.Type, "Provide either a Where filter or a positive Limit for bulk operations.");
        }
    }

    private static void EnsurePositive(object args, string name, QueryModel query)
    {
        var value = EnsureProperty(args, name, query, required: false) as int?;
        if (value.HasValue && value.Value <= 0)
        {
            throw new QueryValidationException(query.ModelName, query.Type, $"{name} must be greater than zero when provided.");
        }
    }

    private static void EnsureNonNegative(object args, string name, QueryModel query)
    {
        var value = EnsureProperty(args, name, query, required: false) as int?;
        if (value.HasValue && value.Value < 0)
        {
            throw new QueryValidationException(query.ModelName, query.Type, $"{name} must be non-negative.");
        }
    }

    private static void EnsureEnumerableNotEmpty(object value, string name, QueryModel query)
    {
        if (value is not IEnumerable enumerable)
        {
            throw new QueryValidationException(query.ModelName, query.Type, $"Property '{name}' must be an enumerable collection.");
        }

        var enumerator = enumerable.GetEnumerator();
        try
        {
            if (!enumerator.MoveNext())
            {
                throw new QueryValidationException(query.ModelName, query.Type, $"Property '{name}' cannot be empty.");
            }
        }
        finally
        {
            (enumerator as IDisposable)?.Dispose();
        }
    }

    private static void EnsureAggregateSelection(object args, QueryModel query)
    {
        var aggregate = EnsureProperty(args, "Aggregate", query, required: false);
        if (aggregate is null)
        {
            throw new QueryValidationException(query.ModelName, query.Type, "Aggregate queries require an Aggregate selector object specifying Count, Min, Max, Avg, or Sum.");
        }

        var count = EnsureProperty(aggregate, "Count", query, required: false) as bool?;
        var min = EnsureProperty(aggregate, "Min", query, required: false);
        var max = EnsureProperty(aggregate, "Max", query, required: false);
        var avg = EnsureProperty(aggregate, "Avg", query, required: false);
        var sum = EnsureProperty(aggregate, "Sum", query, required: false);

        var hasAny = (count is true) || min is not null || max is not null || avg is not null || sum is not null;
        if (!hasAny)
        {
            throw new QueryValidationException(query.ModelName, query.Type, "Aggregate selectors require at least one selection (Aggregate.Count, Aggregate.Min, Aggregate.Max, Aggregate.Avg, or Aggregate.Sum).");
        }
    }

    private static void ValidateGroupBy(QueryModel query)
    {
        var args = EnsureArgs(query);

        var by = EnsureProperty(args, "By", query, required: true)
            ?? throw new QueryValidationException(query.ModelName, query.Type, "Property 'By' is required for groupBy.");
        EnsureEnumerableNotEmpty(by, "By", query);

        EnsureProperty(args, "Where", query, required: false);
        var orderBy = EnsureProperty(args, "OrderBy", query, required: false);
        EnsureProperty(args, "Having", query, required: false);
        EnsureNonNegative(args, "Skip", query);
        EnsurePositive(args, "Take", query);
        EnsureProperty(args, "_count", query, required: false);
        EnsureProperty(args, "_min", query, required: false);
        EnsureProperty(args, "_max", query, required: false);
        EnsureProperty(args, "_avg", query, required: false);
        EnsureProperty(args, "_sum", query, required: false);

        var take = EnsureProperty(args, "Take", query, required: false) as int?;
        var skip = EnsureProperty(args, "Skip", query, required: false) as int?;
        if ((take.HasValue || (skip.HasValue && skip.Value > 0)) && orderBy is null)
        {
            throw new QueryValidationException(query.ModelName, query.Type, "OrderBy is required when using Take or Skip with groupBy.");
        }
    }
}
