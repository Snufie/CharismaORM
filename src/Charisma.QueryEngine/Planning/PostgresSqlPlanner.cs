using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Planning;

/// <summary>
/// PostgreSQL-specific planner that converts <see cref="QueryModel"/> instances into parameterized SQL plans.
/// Execution/materialization is delegated to provider executors.
/// </summary>
public sealed class PostgresSqlPlanner : ISqlPlanner
{
    private readonly IReadOnlyDictionary<string, ModelMetadata> _metadata;
    private static bool _preserveIdentifierCasing;
    private readonly int _maxNestingDepth;
    private readonly IReadOnlyDictionary<string, object?>? _globalOmit;

    public PostgresSqlPlanner(IReadOnlyDictionary<string, ModelMetadata> metadata, bool preserveIdentifierCasing = false, int maxNestingDepth = 12, IReadOnlyDictionary<string, object?>? globalOmit = null)
    {
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        _preserveIdentifierCasing = preserveIdentifierCasing;
        _maxNestingDepth = maxNestingDepth <= 0 ? 12 : maxNestingDepth;
        _globalOmit = globalOmit;
    }

    public SqlPlan Plan(QueryModel query)
    {
        ArgumentNullException.ThrowIfNull(query);

        return query.Type switch
        {
            QueryType.FindUnique => PlanFindUnique(query),
            QueryType.FindFirst => PlanFindFirst(query),
            QueryType.FindMany => PlanFindMany(query),
            QueryType.Count => PlanCount(query),
            QueryType.Aggregate => PlanAggregate(query),
            QueryType.GroupBy => PlanGroupBy(query),
            _ => throw new NotSupportedException($"Query type '{query.Type}' is not supported by PostgresSqlPlanner.")
        };
    }

    private SqlPlan PlanFindUnique(QueryModel query)
    {
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetRequiredProperty(args, "Where");
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");
        var userOmit = GetProperty(args, "Omit");
        EnsureSelectIncludeOmitExclusivity(select, include, userOmit);
        var omit = BuildEffectiveOmit(meta.Name, userOmit, select);
        EnforceNestingDepth(meta, select, omit, include);

        var includes = BuildIncludePlans(meta, include);
        var columns = BuildSelectColumns(meta, select, omit, includePrimaryKey: includes.Count > 0);
        var parameters = new List<SqlParameterValue>();
        var paramCtx = new ParameterContext();
        var whereSql = BuildWhereUnique(meta, where, parameters, paramCtx, "t0");

        var selectFragments = new List<string>();
        AppendSelectColumns(selectFragments, meta, columns, "t0", prefix: string.Empty);
        AppendIncludeSelectColumns(selectFragments, includes);
        var joins = BuildJoinFragments(includes);

        var sql = new StringBuilder();
        sql.Append("SELECT ").Append(string.Join(", ", selectFragments));
        sql.Append(" FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");
        foreach (var join in joins)
        {
            sql.Append(' ').Append(join);
        }
        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            sql.Append(" WHERE ").Append(whereSql);
        }
        sql.Append(" LIMIT 1");

        var includeRoot = includes.Count == 0 ? null : new IncludePlan("__root", meta, "t0", "", string.Empty, false, Array.Empty<string>(), Array.Empty<string>(), includes, BuildSelectColumns(meta, selectObj: null, omitObj: null, includePrimaryKey: true));
        return new SqlPlan(SqlPlanKind.QuerySingle, sql.ToString(), parameters, meta, includeRoot);
    }

    private static void AppendAggregateSelectors(List<string> selectFragments, ModelMetadata meta, object? selector, string prefix, bool numericOnly, string tableAlias)
    {
        if (selector is null)
        {
            return;
        }

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            if (numericOnly && !IsNumericClrType(field.ClrType))
            {
                continue;
            }

            var prop = selector.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            var flag = prop?.GetValue(selector) as bool?;
            if (flag != true)
            {
                continue;
            }

            var function = prefix switch
            {
                "min" => "MIN",
                "max" => "MAX",
                "avg" => "AVG",
                "sum" => "SUM",
                _ => throw new InvalidOperationException($"Unsupported aggregate prefix '{prefix}'.")
            };

            selectFragments.Add($"{function}(\"{tableAlias}\".{QuoteIdentifier(field.Name)}) AS {QuoteIdentifier($"{prefix}_{field.Name}")}");
        }
    }

    private static bool IsNumericClrType(string clrType)
    {
        return clrType.Equals("int", StringComparison.OrdinalIgnoreCase)
            || clrType.Equals("double", StringComparison.OrdinalIgnoreCase)
            || clrType.Equals("decimal", StringComparison.OrdinalIgnoreCase);
    }

    private SqlPlan PlanFindMany(QueryModel query)
    {
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetProperty(args, "Where");
        var orderBy = GetProperty(args, "OrderBy") as System.Collections.IEnumerable;
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");
        var userOmit = GetProperty(args, "Omit");
        EnsureSelectIncludeOmitExclusivity(select, include, userOmit);
        var omit = BuildEffectiveOmit(meta.Name, userOmit, select);
        var distinct = GetProperty(args, "Distinct") as System.Collections.IEnumerable;
        var skip = GetProperty(args, "Skip") as int?;
        var take = GetProperty(args, "Take") as int?;
        var cursor = GetProperty(args, "Cursor");
        EnforceNestingDepth(meta, select, omit, include);

        var reverse = take.HasValue && take.Value < 0;
        var effectiveTake = take.HasValue ? Math.Abs(take.Value) : (int?)null;
        var distinctColumns = BuildDistinctColumns(meta, distinct);
        var hasDistinct = distinctColumns.Count > 0;

        var aliasCtx = new AliasContext();
        var includes = BuildIncludePlans(meta, include);
        var columns = BuildSelectColumns(meta, select, omit, includePrimaryKey: includes.Count > 0);
        var parameters = new List<SqlParameterValue>();
        var paramCtx = new ParameterContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t0", ref aliasCtx);
        var orderTerms = BuildOrderTerms(meta, orderBy, "t0", ref aliasCtx, addTieBreaker: true);
        if (reverse)
        {
            orderTerms = InvertOrderTerms(orderTerms);
        }
        if (hasDistinct && orderTerms.Count == 0)
        {
            orderTerms = BuildDefaultOrderTerms(meta);
        }
        var orderSql = BuildOrderSql(orderTerms, meta, "t0", addTieBreaker: true);

        var cursorSql = BuildCursorPredicate(meta, cursor, orderTerms, "t0", parameters, paramCtx);
        if (!string.IsNullOrWhiteSpace(cursorSql))
        {
            whereSql = string.IsNullOrWhiteSpace(whereSql) ? cursorSql : $"({whereSql}) AND ({cursorSql})";
        }

        var selectFragments = new List<string>();
        AppendSelectColumns(selectFragments, meta, columns, "t0", prefix: string.Empty);
        AppendIncludeSelectColumns(selectFragments, includes);
        var joins = BuildJoinFragments(includes);

        var sql = new StringBuilder();
        sql.Append("SELECT ");
        sql.Append(string.Join(", ", selectFragments));
        sql.Append(" FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");
        foreach (var join in joins)
        {
            sql.Append(' ').Append(join);
        }
        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            sql.Append(" WHERE ").Append(whereSql);
        }
        if (!string.IsNullOrWhiteSpace(orderSql))
        {
            sql.Append(" ORDER BY ").Append(orderSql);
        }
        if (!hasDistinct && effectiveTake.HasValue)
        {
            sql.Append(" LIMIT ").Append(effectiveTake.Value);
        }
        if (!hasDistinct && skip.HasValue)
        {
            sql.Append(" OFFSET ").Append(skip.Value);
        }

        var logEnv = Environment.GetEnvironmentVariable("CHARISMA_LOG_SQL");
        var logSql = string.Equals(logEnv?.Trim(), "1", StringComparison.Ordinal)
            || string.Equals(logEnv?.Trim(), "true", StringComparison.OrdinalIgnoreCase);
        if (logSql)
        {
            Console.WriteLine($"[SQL PLAN {query.Type}] {sql}");
            if (parameters.Count > 0)
            {
                foreach (var p in parameters)
                {
                    Console.WriteLine($"  {p.Name} = {p.Value ?? "<null>"}");
                }
            }
        }

        var includeRoot = includes.Count == 0 ? null : new IncludePlan("__root", meta, "t0", "", string.Empty, false, Array.Empty<string>(), Array.Empty<string>(), includes, BuildSelectColumns(meta, selectObj: null, omitObj: null, includePrimaryKey: true));
        return new SqlPlan(SqlPlanKind.QueryMany, sql.ToString(), parameters, meta, includeRoot, hasDistinct ? distinctColumns : null, hasDistinct ? skip : null, hasDistinct ? effectiveTake : null);
    }

    private List<OrderTerm> BuildDefaultOrderTerms(ModelMetadata meta)
    {
        var terms = new List<OrderTerm>();
        if (meta.PrimaryKey is { Fields.Count: > 0 })
        {
            terms.AddRange(meta.PrimaryKey.Fields.Select(f => new OrderTerm(f, "ASC")));
            return terms;
        }

        var firstScalar = meta.Fields.FirstOrDefault(f => f.Kind == FieldKind.Scalar)?.Name;
        if (!string.IsNullOrWhiteSpace(firstScalar))
        {
            terms.Add(new OrderTerm(firstScalar!, "ASC"));
        }

        return terms;
    }

    private SqlPlan PlanFindFirst(QueryModel query)
    {
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetProperty(args, "Where");
        var orderBy = GetProperty(args, "OrderBy") as System.Collections.IEnumerable;
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");
        var userOmit = GetProperty(args, "Omit");
        EnsureSelectIncludeOmitExclusivity(select, include, userOmit);
        var omit = BuildEffectiveOmit(meta.Name, userOmit, select);
        var distinct = GetProperty(args, "Distinct") as System.Collections.IEnumerable;
        var skip = GetProperty(args, "Skip") as int?;
        var take = GetProperty(args, "Take") as int?;
        var cursor = GetProperty(args, "Cursor");
        EnforceNestingDepth(meta, select, omit, include);

        var reverse = take.HasValue && take.Value < 0;
        var effectiveTake = take.HasValue ? Math.Abs(take.Value) : 1;
        var distinctColumns = BuildDistinctColumns(meta, distinct);
        var hasDistinct = distinctColumns.Count > 0;

        var aliasCtx = new AliasContext();
        var includes = BuildIncludePlans(meta, include);
        var columns = BuildSelectColumns(meta, select, omit, includePrimaryKey: includes.Count > 0);
        var parameters = new List<SqlParameterValue>();
        var paramCtx = new ParameterContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t0", ref aliasCtx);
        var orderTerms = BuildOrderTerms(meta, orderBy, "t0", ref aliasCtx);
        if (reverse)
        {
            orderTerms = InvertOrderTerms(orderTerms);
        }
        if (hasDistinct && orderTerms.Count == 0)
        {
            orderTerms = BuildDefaultOrderTerms(meta);
        }
        var orderSql = BuildOrderSql(orderTerms, meta, "t0");

        var cursorSql = BuildCursorPredicate(meta, cursor, orderTerms, "t0", parameters, paramCtx);
        if (!string.IsNullOrWhiteSpace(cursorSql))
        {
            whereSql = string.IsNullOrWhiteSpace(whereSql) ? cursorSql : $"({whereSql}) AND ({cursorSql})";
        }

        var selectFragments = new List<string>();
        AppendSelectColumns(selectFragments, meta, columns, "t0", prefix: string.Empty);
        AppendIncludeSelectColumns(selectFragments, includes);
        var joins = BuildJoinFragments(includes);

        var sql = new StringBuilder();
        sql.Append("SELECT ");
        sql.Append(string.Join(", ", selectFragments));
        sql.Append(" FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");
        foreach (var join in joins)
        {
            sql.Append(' ').Append(join);
        }
        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            sql.Append(" WHERE ").Append(whereSql);
        }
        if (!string.IsNullOrWhiteSpace(orderSql))
        {
            sql.Append(" ORDER BY ").Append(orderSql);
        }
        if (!hasDistinct && effectiveTake > 0)
        {
            sql.Append(" LIMIT ").Append(effectiveTake);
        }
        if (!hasDistinct && skip.HasValue)
        {
            sql.Append(" OFFSET ").Append(skip.Value);
        }

        var includeRoot = includes.Count == 0 ? null : new IncludePlan("__root", meta, "t0", "", string.Empty, false, Array.Empty<string>(), Array.Empty<string>(), includes, BuildSelectColumns(meta, selectObj: null, omitObj: null, includePrimaryKey: true));
        return new SqlPlan(SqlPlanKind.QuerySingle, sql.ToString(), parameters, meta, includeRoot, hasDistinct ? distinctColumns : null, hasDistinct ? skip : null, hasDistinct ? effectiveTake : null);
    }

    private SqlPlan PlanCount(QueryModel query)
    {
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetProperty(args, "Where");
        var distinct = GetProperty(args, "Distinct") as System.Collections.IEnumerable;
        var distinctColumns = BuildDistinctColumns(meta, distinct);
        var parameters = new List<SqlParameterValue>();
        var paramCtx = new ParameterContext();
        var aliasCtx = new AliasContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t0", ref aliasCtx);

        var countSql = new StringBuilder();
        if (distinctColumns.Count > 0)
        {
            var distinctExpr = string.Join(", ", distinctColumns.Select(c => $"\"t0\".{QuoteIdentifier(c)}"));
            countSql.Append("SELECT COUNT(DISTINCT ").Append(distinctExpr).Append(") FROM ")
                .Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");
        }
        else
        {
            countSql.Append("SELECT COUNT(*) FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");
        }

        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            countSql.Append(" WHERE ").Append(whereSql);
        }

        return new SqlPlan(SqlPlanKind.QuerySingle, countSql.ToString(), parameters, null, null);
    }

    /// <summary>
    /// Plans group-by queries with aggregate selectors.
    /// </summary>
    private SqlPlan PlanGroupBy(QueryModel query)
    {
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetProperty(args, "Where");
        var orderBy = GetProperty(args, "OrderBy") as System.Collections.IEnumerable;
        var having = GetProperty(args, "Having");
        var skip = GetProperty(args, "Skip") as int?;
        var take = GetProperty(args, "Take") as int?;
        var by = GetProperty(args, "By") as System.Collections.IEnumerable ?? throw new NotSupportedException("groupBy requires a non-empty 'By' list.");

        var count = GetProperty(args, "_count") as bool?;
        var min = GetProperty(args, "_min");
        var max = GetProperty(args, "_max");
        var avg = GetProperty(args, "_avg");
        var sum = GetProperty(args, "_sum");

        var groupColumns = new List<string>();
        foreach (var item in by)
        {
            if (item is not string name)
            {
                throw new NotSupportedException("By values must be scalar field names.");
            }

            var field = meta.Fields.FirstOrDefault(f => f.Kind == FieldKind.Scalar && string.Equals(f.Name, name, StringComparison.Ordinal));
            if (field is null)
            {
                throw new NotSupportedException($"GroupBy field '{name}' is not a scalar field on model '{meta.Name}'.");
            }

            if (!groupColumns.Contains(field.Name, StringComparer.Ordinal))
            {
                groupColumns.Add(field.Name);
            }
        }

        var selectFragments = new List<string>();
        foreach (var col in groupColumns)
        {
            // Alias grouped keys using the original field casing so materialization can map them back directly.
            selectFragments.Add($"\"t0\".{QuoteIdentifier(col)} AS \"{col}\"");
        }

        if (count == true)
        {
            selectFragments.Add("COUNT(*) AS \"count\"");
        }

        AppendAggregateSelectors(selectFragments, meta, min, "min", numericOnly: false, "t0");
        AppendAggregateSelectors(selectFragments, meta, max, "max", numericOnly: false, "t0");
        AppendAggregateSelectors(selectFragments, meta, avg, "avg", numericOnly: true, "t0");
        AppendAggregateSelectors(selectFragments, meta, sum, "sum", numericOnly: true, "t0");

        if (selectFragments.Count == 0)
        {
            throw new NotSupportedException("GroupBy requires at least one grouping field or aggregate selector.");
        }

        var parameters = new List<SqlParameterValue>();
        var paramCtx = new ParameterContext();
        var aliasCtx = new AliasContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t0", ref aliasCtx);
        var havingSql = BuildWhere(meta, having, parameters, paramCtx, "t0", ref aliasCtx);
        var orderTerms = BuildOrderTerms(meta, orderBy, "t0", ref aliasCtx, addTieBreaker: false);
        var orderSql = BuildOrderSql(orderTerms, meta, "t0", addTieBreaker: false);

        var sql = new StringBuilder();
        sql.Append("SELECT ").Append(string.Join(", ", selectFragments));
        sql.Append(" FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");

        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            sql.Append(" WHERE ").Append(whereSql);
        }

        if (groupColumns.Count > 0)
        {
            sql.Append(" GROUP BY ").Append(string.Join(", ", groupColumns.Select(c => $"\"t0\".{QuoteIdentifier(c)}")));
        }

        if (!string.IsNullOrWhiteSpace(havingSql))
        {
            sql.Append(" HAVING ").Append(havingSql);
        }

        if (!string.IsNullOrWhiteSpace(orderSql))
        {
            sql.Append(" ORDER BY ").Append(orderSql);
        }

        if (take.HasValue)
        {
            sql.Append(" LIMIT ").Append(Math.Abs(take.Value));
        }

        if (skip.HasValue)
        {
            sql.Append(" OFFSET ").Append(skip.Value);
        }

        return new SqlPlan(SqlPlanKind.QueryMany, sql.ToString(), parameters, meta, null);
    }

    /// <summary>
    /// Plans aggregate projections (count/min/max/avg/sum) over a filtered model set.
    /// </summary>
    private SqlPlan PlanAggregate(QueryModel query)
    {
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetProperty(args, "Where");
        var orderBy = GetProperty(args, "OrderBy") as System.Collections.IEnumerable;
        var cursor = GetProperty(args, "Cursor");
        var skip = GetProperty(args, "Skip") as int?;
        var take = GetProperty(args, "Take") as int?;
        var distinct = GetProperty(args, "Distinct") as System.Collections.IEnumerable;
        var distinctColumns = BuildDistinctColumns(meta, distinct);

        var aggregate = GetProperty(args, "Aggregate");
        if (aggregate is null)
        {
            throw new NotSupportedException("Aggregate selectors must be provided via the Aggregate property.");
        }

        var count = GetProperty(aggregate, "Count") as bool?;
        var min = GetProperty(aggregate, "Min");
        var max = GetProperty(aggregate, "Max");
        var avg = GetProperty(aggregate, "Avg");
        var sum = GetProperty(aggregate, "Sum");

        var sourceAlias = distinctColumns.Count > 0 ? "src" : "t0";
        var selectFragments = new List<string>();
        if (count == true)
        {
            selectFragments.Add("COUNT(*) AS \"count\"");
        }

        AppendAggregateSelectors(selectFragments, meta, min, "min", numericOnly: false, sourceAlias);
        AppendAggregateSelectors(selectFragments, meta, max, "max", numericOnly: false, sourceAlias);
        AppendAggregateSelectors(selectFragments, meta, avg, "avg", numericOnly: true, sourceAlias);
        AppendAggregateSelectors(selectFragments, meta, sum, "sum", numericOnly: true, sourceAlias);

        if (selectFragments.Count == 0)
        {
            throw new NotSupportedException("Aggregate queries must request at least one Aggregate selector (Count, Min, Max, Avg, Sum).");
        }

        var parameters = new List<SqlParameterValue>();
        var paramCtx = new ParameterContext();
        var aliasCtx = new AliasContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t0", ref aliasCtx);
        var orderTerms = distinctColumns.Count > 0 ? BuildOrderTerms(meta, orderBy, "t0", ref aliasCtx) : new List<OrderTerm>();
        var orderSql = distinctColumns.Count > 0 ? BuildOrderSql(orderTerms, meta, "t0") : string.Empty;
        if (distinctColumns.Count > 0)
        {
            var cursorSql = BuildCursorPredicate(meta, cursor, orderTerms, "t0", parameters, paramCtx);
            if (!string.IsNullOrWhiteSpace(cursorSql))
            {
                whereSql = string.IsNullOrWhiteSpace(whereSql) ? cursorSql : $"({whereSql}) AND ({cursorSql})";
            }
        }

        var sql = new StringBuilder();

        if (distinctColumns.Count > 0)
        {
            var inner = new StringBuilder();
            inner.Append("SELECT DISTINCT ON (")
                .Append(string.Join(", ", distinctColumns.Select(c => $"\"t0\".{QuoteIdentifier(c)}")))
                .Append(") * FROM ")
                .Append(QuoteIdentifier(meta.Name))
                .Append(" AS \"t0\"");

            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                inner.Append(" WHERE ").Append(whereSql);
            }

            var distinctOrderParts = new List<string>(distinctColumns.Select(c => $"\"t0\".{QuoteIdentifier(c)}"));
            if (!string.IsNullOrWhiteSpace(orderSql))
            {
                distinctOrderParts.Add(orderSql);
            }

            if (distinctOrderParts.Count > 0)
            {
                inner.Append(" ORDER BY ").Append(string.Join(", ", distinctOrderParts));
            }

            if (take.HasValue)
            {
                inner.Append(" LIMIT ").Append(take.Value);
            }
            if (skip.HasValue)
            {
                inner.Append(" OFFSET ").Append(skip.Value);
            }

            sql.Append("SELECT ").Append(string.Join(", ", selectFragments));
            sql.Append(" FROM (").Append(inner).Append(") AS \"src\"");
            sourceAlias = "src";
        }
        else
        {
            sql.Append("SELECT ").Append(string.Join(", ", selectFragments));
            sql.Append(" FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");

            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
        }

        return new SqlPlan(SqlPlanKind.QuerySingle, sql.ToString(), parameters, null, null);
    }

    private ModelMetadata GetModelMetadata(string name)
    {
        if (_metadata.TryGetValue(name, out var meta))
        {
            return meta;
        }
        throw new InvalidOperationException($"Model metadata for '{name}' was not found.");
    }

    private static object? GetProperty(object obj, string name)
    {
        return obj.GetType().GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)?.GetValue(obj);
    }

    private static object GetRequiredProperty(object obj, string name)
    {
        var value = GetProperty(obj, name);
        return value ?? throw new InvalidOperationException($"Required property '{name}' was not provided on args object of type '{obj.GetType().Name}'.");
    }

    private static void EnsureSelectIncludeOmitExclusivity(object? select, object? include, object? omit)
    {
        if ((select is not null && include is not null) || (select is not null && omit is not null) || (include is not null && omit is not null))
        {
            throw new NotSupportedException("Select, Include, and Omit cannot be combined; provide at most one of them.");
        }
    }

    private static IReadOnlyList<string> BuildSelectColumns(ModelMetadata meta, object? selectObj, object? omitObj, bool includePrimaryKey = false)
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
            Dictionary<string, bool?>? selectMap = null;
            if (selectObj is IReadOnlyDictionary<string, object?> objDict)
            {
                selectMap = new Dictionary<string, bool?>(StringComparer.OrdinalIgnoreCase);
                foreach (var kvp in objDict)
                {
                    if (kvp.Value is bool flag)
                    {
                        selectMap[kvp.Key] = flag;
                    }
                }
            }
            else if (selectObj is System.Collections.IDictionary legacyDict)
            {
                selectMap = new Dictionary<string, bool?>(StringComparer.OrdinalIgnoreCase);
                foreach (System.Collections.DictionaryEntry entry in legacyDict)
                {
                    if (entry.Key is string key && entry.Value is bool flag)
                    {
                        selectMap[key] = flag;
                    }
                }
            }

            foreach (var field in meta.Fields)
            {
                if (field.Kind != FieldKind.Scalar)
                {
                    continue;
                }

                bool? val = null;
                if (selectMap is not null && selectMap.TryGetValue(field.Name, out var mapped))
                {
                    val = mapped;
                }
                else
                {
                    var prop = selectObj.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
                    val = prop?.GetValue(selectObj) as bool?;
                }

                if (val == true)
                {
                    selected.Add(field.Name);
                }
            }
        }
        else if (omitObj is not null)
        {
            var omitMap = ToOmitMap(omitObj);
            foreach (var field in meta.Fields)
            {
                if (field.Kind != FieldKind.Scalar)
                {
                    continue;
                }

                bool? val = null;
                if (omitMap is not null && omitMap.TryGetValue(field.Name, out var flag))
                {
                    val = flag;
                }
                else
                {
                    var prop = omitObj.GetType().GetProperty(field.Name, BindingFlags.Public | BindingFlags.Instance);
                    val = prop?.GetValue(omitObj) as bool?;
                }

                if (val != true)
                {
                    selected.Add(field.Name);
                }
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

        if (selected.Count == 0)
        {
            throw new NotSupportedException("Select/Omit mask produced no scalar fields; request at least one scalar or remove the mask.");
        }

        return selected;
    }

    private object? BuildEffectiveOmit(string modelName, object? userOmitObj, object? selectObj)
    {
        if (selectObj is not null)
        {
            return null;
        }

        var globalMap = ToOmitMap(GetDefaultOmit(modelName, selectObj));
        var userMap = ToOmitMap(userOmitObj);

        if (userMap is null || userMap.Count == 0)
        {
            return globalMap;
        }

        if (globalMap is null || globalMap.Count == 0)
        {
            return userMap;
        }

        var merged = new Dictionary<string, bool?>(globalMap, StringComparer.Ordinal);
        foreach (var kvp in userMap)
        {
            if (!kvp.Value.HasValue)
            {
                continue;
            }

            merged[kvp.Key] = kvp.Value;
        }

        return merged;
    }

    private static Dictionary<string, bool?>? ToOmitMap(object? omitObj)
    {
        if (omitObj is null)
        {
            return null;
        }

        if (omitObj is IReadOnlyDictionary<string, bool?> boolDict)
        {
            return new Dictionary<string, bool?>(boolDict, StringComparer.Ordinal);
        }

        if (omitObj is IReadOnlyDictionary<string, object?> objDict)
        {
            var map = new Dictionary<string, bool?>(StringComparer.Ordinal);
            foreach (var kvp in objDict)
            {
                if (kvp.Value is bool b)
                {
                    map[kvp.Key] = b;
                }
                else if (kvp.Value is null)
                {
                    map[kvp.Key] = null;
                }
                else if (kvp.Value is bool)
                {
                    map[kvp.Key] = (bool)kvp.Value;
                }
            }

            return map.Count == 0 ? null : map;
        }

        var props = omitObj.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var result = new Dictionary<string, bool?>(StringComparer.Ordinal);
        foreach (var prop in props)
        {
            if (prop.PropertyType != typeof(bool) && prop.PropertyType != typeof(bool?))
            {
                continue;
            }

            var val = prop.GetValue(omitObj) as bool?;
            result[prop.Name] = val;
        }

        return result.Count == 0 ? null : result;
    }

    private static List<string> BuildDistinctColumns(ModelMetadata meta, System.Collections.IEnumerable? distinct)
    {
        var columns = new List<string>();
        if (distinct is null)
        {
            return columns;
        }

        foreach (var item in distinct)
        {
            if (item is not string name)
            {
                throw new NotSupportedException("Distinct values must be string field names.");
            }

            var field = meta.Fields.FirstOrDefault(f => f.Kind == FieldKind.Scalar && string.Equals(f.Name, name, StringComparison.Ordinal));
            if (field is null)
            {
                throw new NotSupportedException($"Distinct field '{name}' is not a scalar field on model '{meta.Name}'.");
            }

            if (!columns.Contains(field.Name, StringComparer.Ordinal))
            {
                columns.Add(field.Name);
            }
        }

        return columns;
    }

    private List<IncludePlan> BuildIncludePlans(ModelMetadata meta, object? includeObj)
    {
        var aliasCounter = 0;
        return BuildIncludePlans(meta, includeObj, "t0", ref aliasCounter, 0);
    }

    private List<IncludePlan> BuildIncludePlans(ModelMetadata meta, object? includeObj, string parentAlias, ref int aliasCounter, int depth)
    {
        var plans = new List<IncludePlan>();
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
            if (!TryGetIncludeValue(includeObj, relationField.Name, out var value) || value is null)
            {
                continue;
            }

            var childMeta = GetModelMetadata(relationField.ClrType);
            var childSelect = GetProperty(value, "Select");
            var childUserOmit = GetProperty(value, "Omit");
            EnsureSelectIncludeOmitExclusivity(childSelect, null, childUserOmit);
            var childOmit = BuildEffectiveOmit(childMeta.Name, childUserOmit, childSelect);
            var columns = BuildSelectColumns(childMeta, childSelect, childOmit, includePrimaryKey: true);
            var mapping = ResolveRelationMapping(meta, childMeta, relationField);
            var childAlias = $"t{++aliasCounter}";
            var childPrefix = $"{childAlias}__";
            var nested = BuildIncludePlans(childMeta, value, childAlias, ref aliasCounter, depth + 1);

            plans.Add(new IncludePlan(
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

    private static bool TryGetIncludeValue(object includeObj, string relationName, out object? value)
    {
        var prop = includeObj.GetType().GetProperty(relationName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (prop is not null)
        {
            value = prop.GetValue(includeObj);
            if (value is not null)
            {
                return true;
            }
        }

        if (includeObj is IReadOnlyDictionary<string, object?> roDict && roDict.TryGetValue(relationName, out value))
        {
            return value is not null;
        }

        if (includeObj is System.Collections.IDictionary legacyDict)
        {
            foreach (System.Collections.DictionaryEntry entry in legacyDict)
            {
                if (entry.Key is string key && string.Equals(key, relationName, StringComparison.OrdinalIgnoreCase))
                {
                    value = entry.Value;
                    return value is not null;
                }
            }
        }

        value = null;
        return false;
    }

    private object? GetDefaultOmit(string modelName, object? selectObj)
    {
        if (selectObj is not null || _globalOmit is null)
        {
            return null;
        }

        return _globalOmit.TryGetValue(modelName, out var omit) ? omit : null;
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

    private static void AppendSelectColumns(List<string> fragments, ModelMetadata meta, IReadOnlyList<string> columns, string alias, string prefix)
    {
        foreach (var column in columns)
        {
            var aliasLabel = string.IsNullOrEmpty(prefix) ? QuoteIdentifier(column) : QuoteIdentifier($"{prefix}{column}");
            fragments.Add($"\"{alias}\".{QuoteIdentifier(column)} AS {aliasLabel}");
        }
    }

    private void AppendIncludeSelectColumns(List<string> fragments, IReadOnlyList<IncludePlan> includes)
    {
        foreach (var include in includes)
        {
            AppendSelectColumns(fragments, include.Meta, include.Columns, include.Alias, include.ColumnPrefix);
            AppendIncludeSelectColumns(fragments, include.Children);
        }
    }

    private static IReadOnlyList<string> BuildJoinFragments(IReadOnlyList<IncludePlan> includes)
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

    private static bool TryGetWhereFieldValue(object whereObj, string fieldName, out object? value)
    {
        var prop = whereObj.GetType().GetProperty(fieldName, BindingFlags.Public | BindingFlags.Instance);
        if (prop is not null)
        {
            value = prop.GetValue(whereObj);
            return true;
        }

        if (whereObj is IReadOnlyDictionary<string, object?> roDict && roDict.TryGetValue(fieldName, out value))
        {
            return true;
        }

        if (whereObj is IDictionary<string, object?> dict && dict.TryGetValue(fieldName, out value))
        {
            return true;
        }

        if (whereObj is System.Collections.IDictionary legacyDict)
        {
            foreach (System.Collections.DictionaryEntry entry in legacyDict)
            {
                if (entry.Key is string key && string.Equals(key, fieldName, StringComparison.OrdinalIgnoreCase))
                {
                    value = entry.Value;
                    return true;
                }
            }
        }

        value = null;
        return false;
    }

    private static string BuildWhereUnique(ModelMetadata meta, object whereObj, List<SqlParameterValue> parameters, ParameterContext paramCtx, string tableAlias)
    {
        ArgumentNullException.ThrowIfNull(meta);
        ArgumentNullException.ThrowIfNull(whereObj);
        ArgumentNullException.ThrowIfNull(paramCtx);

        var clauses = new List<string>();
        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            if (!TryGetWhereFieldValue(whereObj, field.Name, out var value))
            {
                continue;
            }
            if (value is null)
            {
                continue;
            }

            // Skip default value-type placeholders (e.g., default Guid/DateTime) so optional fields
            // not explicitly set on WhereUnique inputs do not become unintended predicates.
            if (IsDefaultValue(value))
            {
                continue;
            }

            var paramName = paramCtx.Next();
            clauses.Add($"\"{tableAlias}\".{QuoteIdentifier(field.Name)} = {paramName}");
            parameters.Add(new SqlParameterValue(paramName, value));
        }

        if (clauses.Count == 0)
        {
            throw new InvalidOperationException($"Where predicate for '{meta.Name}' produced no filters; refusing to generate a non-unique plan.");
        }

        return string.Join(" AND ", clauses);
    }

    private string BuildWhere(ModelMetadata meta, object? whereObj, List<SqlParameterValue> parameters, ParameterContext paramCtx, string tableAlias, ref AliasContext aliasCtx)
    {
        if (whereObj is null)
        {
            return string.Empty;
        }

        var clauses = new List<string>();
        var type = whereObj.GetType();

        var andProp = type.GetProperty("AND");
        var andParts = new List<string>();
        foreach (var item in NormalizeLogicalConditions(andProp?.GetValue(whereObj)))
        {
            var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
            if (!string.IsNullOrWhiteSpace(fragment))
            {
                andParts.Add(fragment);
            }
        }
        if (andParts.Count > 0)
        {
            clauses.Add($"( {string.Join(" AND ", andParts)} )");
        }

        var orProp = type.GetProperty("OR");
        var orParts = new List<string>();
        foreach (var item in NormalizeLogicalConditions(orProp?.GetValue(whereObj)))
        {
            var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
            if (!string.IsNullOrWhiteSpace(fragment))
            {
                orParts.Add(fragment);
            }
        }
        if (orParts.Count > 0)
        {
            clauses.Add($"( {string.Join(" OR ", orParts)} )");
        }

        var xorProp = type.GetProperty("XOR");
        var xorParts = new List<string>();
        foreach (var item in NormalizeLogicalConditions(xorProp?.GetValue(whereObj)))
        {
            var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
            if (!string.IsNullOrWhiteSpace(fragment))
            {
                xorParts.Add(fragment);
            }
        }
        if (xorParts.Count > 0)
        {
            var orTerms = new List<string>(xorParts.Count);
            for (int i = 0; i < xorParts.Count; i++)
            {
                var positives = xorParts[i];
                var negatives = xorParts.Where((_, idx) => idx != i).Select(p => $"NOT ({p})");
                var term = negatives.Any() ? $"({positives}) AND {string.Join(" AND ", negatives)}" : positives;
                orTerms.Add($"( {term} )");
            }

            clauses.Add($"( {string.Join(" OR ", orTerms)} )");

            var logEnv = Environment.GetEnvironmentVariable("CHARISMA_LOG_SQL");
            var logSql = string.Equals(logEnv?.Trim(), "1", StringComparison.Ordinal)
                || string.Equals(logEnv?.Trim(), "true", StringComparison.OrdinalIgnoreCase);
            if (logSql)
            {
                Console.WriteLine($"[SQL XOR] {string.Join(" | ", xorParts)}");
            }
        }

        var notProp = type.GetProperty("NOT");
        var notParts = new List<string>();
        foreach (var item in NormalizeLogicalConditions(notProp?.GetValue(whereObj)))
        {
            var fragment = BuildWhere(meta, item, parameters, paramCtx, tableAlias, ref aliasCtx);
            if (!string.IsNullOrWhiteSpace(fragment))
            {
                notParts.Add($"NOT ({fragment})");
            }
        }
        if (notParts.Count > 0)
        {
            clauses.Add(string.Join(" AND ", notParts));
        }

        foreach (var field in meta.Fields.Where(f => f.Kind == FieldKind.Scalar))
        {
            if (!TryGetWhereFieldValue(whereObj, field.Name, out var value) || value is null)
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

    private static IEnumerable<object> NormalizeLogicalConditions(object? value)
    {
        if (value is null)
        {
            yield break;
        }

        if (value is string)
        {
            yield return value;
            yield break;
        }

        if (value is System.Collections.IEnumerable enumerable)
        {
            foreach (var item in enumerable)
            {
                yield return item!;
            }

            yield break;
        }

        yield return value;
    }

    private string BuildRelationFilterClause(ModelMetadata parentMeta, FieldMetadata relationField, object filterValue, List<SqlParameterValue> parameters, ParameterContext paramCtx, string parentAlias, ref AliasContext aliasCtx)
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

    private static string BuildScalarFilterClause(FieldMetadata field, object filterValue, List<SqlParameterValue> parameters, ParameterContext paramCtx, string tableAlias)
    {
        if (string.Equals(field.ClrType, "Json", StringComparison.Ordinal))
        {
            return BuildJsonFilterClause(field, filterValue, parameters, paramCtx, tableAlias);
        }

        if (IsSimpleValue(filterValue))
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, filterValue));
            return $"\"{tableAlias}\".{QuoteIdentifier(field.Name)} = {paramName}";
        }

        var clauses = new List<string>();
        var type = filterValue.GetType();
        var column = $"\"{tableAlias}\".{QuoteIdentifier(field.Name)}";
        var isStringField = string.Equals(field.ClrType, "string", StringComparison.OrdinalIgnoreCase)
            || string.Equals(field.ClrType, "String", StringComparison.OrdinalIgnoreCase);
        var likeOperator = "ILIKE";
        if (isStringField)
        {
            var modeProp = type.GetProperty("Mode", BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            var mode = modeProp?.GetValue(filterValue)?.ToString();
            if (string.Equals(mode, "Sensitive", StringComparison.OrdinalIgnoreCase))
            {
                likeOperator = "LIKE";
            }
        }

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
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("Equals")!.GetValue(filterValue)));
            return $"{column} = {paramName}";
        });

        AddIfNotNull("In", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("In")!.GetValue(filterValue)));
            return $"{column} = ANY({paramName})";
        });

        AddIfNotNull("NotIn", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("NotIn")!.GetValue(filterValue)));
            return $"NOT ({column} = ANY({paramName}))";
        });

        AddIfNotNull("Gt", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("Gt")!.GetValue(filterValue)));
            return $"{column} > {paramName}";
        });

        AddIfNotNull("Gte", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("Gte")!.GetValue(filterValue)));
            return $"{column} >= {paramName}";
        });

        AddIfNotNull("Lt", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("Lt")!.GetValue(filterValue)));
            return $"{column} < {paramName}";
        });

        AddIfNotNull("Lte", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("Lte")!.GetValue(filterValue)));
            return $"{column} <= {paramName}";
        });

        AddIfNotNull("Contains", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, $"%{type.GetProperty("Contains")!.GetValue(filterValue)}%"));
            return $"{column} {likeOperator} {paramName}";
        });

        AddIfNotNull("StartsWith", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, $"{type.GetProperty("StartsWith")!.GetValue(filterValue)}%"));
            return $"{column} {likeOperator} {paramName}";
        });

        AddIfNotNull("EndsWith", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, $"%{type.GetProperty("EndsWith")!.GetValue(filterValue)}"));
            return $"{column} {likeOperator} {paramName}";
        });

        AddIfNotNull("Not", () =>
        {
            var notVal = type.GetProperty("Not")!.GetValue(filterValue);
            var clause = BuildScalarFilterClause(field, notVal!, parameters, paramCtx, tableAlias);
            return $"NOT ({clause})";
        });

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonFilterClause(FieldMetadata field, object filterValue, List<SqlParameterValue> parameters, ParameterContext paramCtx, string tableAlias)
    {
        var column = $"\"{tableAlias}\".{QuoteIdentifier(field.Name)}";

        if (IsSimpleValue(filterValue))
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, filterValue));
            return $"{column} = {paramName}::jsonb";
        }

        var clauses = new List<string>();
        var type = filterValue.GetType();

        object? FirstPropValue(params string[] names)
        {
            foreach (var name in names)
            {
                var prop = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance);
                if (prop is null)
                {
                    continue;
                }

                var val = prop.GetValue(filterValue);
                if (val is not null)
                {
                    return val;
                }
            }

            return null;
        }

        void AddIfNotNull(Func<object, string> builder, params string[] propNames)
        {
            var val = FirstPropValue(propNames);
            if (val is null)
            {
                return;
            }

            var built = builder(val);
            if (!string.IsNullOrWhiteSpace(built))
            {
                clauses.Add(built);
            }
        }

        AddIfNotNull(val =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, val));
            return $"{column} = {paramName}::jsonb";
        }, "Equals");

        AddIfNotNull(val =>
        {
            var clause = BuildJsonFilterClause(field, val, parameters, paramCtx, tableAlias);
            return $"NOT ({clause})";
        }, "Not");

        AddIfNotNull(val =>
        {
            return BuildJsonPathFilterClause(column, val, parameters, paramCtx);
        }, "path");

        AddIfNotNull(val =>
        {
            return BuildJsonArrayFilterClause(BuildJsonPathExpression(column, Array.Empty<string>()), val, parameters, paramCtx);
        }, "array_contains");

        AddIfNotNull(val =>
        {
            return BuildJsonStringFilterClause(column, Array.Empty<string>(), val, parameters, paramCtx);
        }, "stringFilter");

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonPathFilterClause(string column, object pathFilter, List<SqlParameterValue> parameters, ParameterContext paramCtx)
    {
        var type = pathFilter.GetType();

        object? FirstPropValue(params string[] names)
        {
            foreach (var name in names)
            {
                var prop = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance);
                if (prop is null)
                {
                    continue;
                }

                var val = prop.GetValue(pathFilter);
                if (val is not null)
                {
                    return val;
                }
            }

            return null;
        }

        var segmentsObj = FirstPropValue("path") as System.Collections.IEnumerable;
        var segments = segmentsObj?.Cast<object?>().Select(s => s?.ToString() ?? string.Empty).Where(s => s is not null).ToList() ?? new List<string>();

        var jsonExpr = BuildJsonPathExpression(column, segments);

        var clauses = new List<string>();

        void AddIfNotNull(Func<object, string> builder, params string[] propNames)
        {
            var val = FirstPropValue(propNames);
            if (val is null)
            {
                return;
            }

            var built = builder(val);
            if (!string.IsNullOrWhiteSpace(built))
            {
                clauses.Add(built);
            }
        }

        AddIfNotNull(val =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, val));
            return $"{jsonExpr} = {paramName}::jsonb";
        }, "Equals");

        AddIfNotNull(val =>
        {
            var clause = BuildJsonPathFilterClause(column, val, parameters, paramCtx);
            return $"NOT ({clause})";
        }, "Not");

        AddIfNotNull(val =>
        {
            return BuildJsonArrayFilterClause(jsonExpr, val, parameters, paramCtx);
        }, "array_contains");

        AddIfNotNull(val =>
        {
            return BuildJsonStringFilterClause(column, segments, val, parameters, paramCtx);
        }, "stringFilter");

        return string.Join(" AND ", clauses);
    }

    private static string BuildJsonArrayFilterClause(string jsonExpr, object arrayFilter, List<SqlParameterValue> parameters, ParameterContext paramCtx)
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
            parameters.Add(new SqlParameterValue(paramName, payload));
            return $"{ensureArray} AND {jsonExpr} @> {paramName}::jsonb";
        });

        AddIfNotNull("HasEvery", () =>
        {
            var values = type.GetProperty("HasEvery")!.GetValue(arrayFilter) as System.Collections.IEnumerable;
            if (values is null)
            {
                return string.Empty;
            }

            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, BuildArrayPayload(values)));
            return $"{ensureArray} AND {jsonExpr} @> {paramName}::jsonb";
        });

        AddIfNotNull("HasSome", () =>
        {
            var values = type.GetProperty("HasSome")!.GetValue(arrayFilter) as System.Collections.IEnumerable;
            if (values is null)
            {
                return string.Empty;
            }

            var predicates = new List<string>();
            foreach (var val in values)
            {
                var paramName = paramCtx.Next();
                parameters.Add(new SqlParameterValue(paramName, WrapArrayPayload(val)));
                predicates.Add($"{jsonExpr} @> {paramName}::jsonb");
            }

            if (predicates.Count == 0)
            {
                return string.Empty;
            }

            return $"{ensureArray} AND (" + string.Join(" OR ", predicates) + ")";
        });

        AddIfNotNull("Length", () =>
        {
            var paramName = paramCtx.Next();
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("Length")!.GetValue(arrayFilter)));
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

    private static string BuildJsonStringFilterClause(string column, IReadOnlyList<string> segments, object stringFilter, List<SqlParameterValue> parameters, ParameterContext paramCtx)
    {
        var textExpr = BuildJsonTextExpression(column, segments);
        var clauses = new List<string>();
        var type = stringFilter.GetType();
        var mode = type.GetProperty("Mode")?.GetValue(stringFilter)?.ToString();
        var op = string.Equals(mode, "Sensitive", StringComparison.OrdinalIgnoreCase) ? "LIKE" : "ILIKE";

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
            parameters.Add(new SqlParameterValue(paramName, type.GetProperty("Equals")!.GetValue(stringFilter)));
            return op == "LIKE" ? $"{textExpr} = {paramName}" : $"{textExpr} {op} {paramName}";
        });

        AddIfNotNull("Contains", () =>
        {
            var paramName = paramCtx.Next();
            var raw = type.GetProperty("Contains")!.GetValue(stringFilter)?.ToString() ?? string.Empty;
            parameters.Add(new SqlParameterValue(paramName, $"%{raw}%"));
            return $"{textExpr} {op} {paramName}";
        });

        AddIfNotNull("StartsWith", () =>
        {
            var paramName = paramCtx.Next();
            var raw = type.GetProperty("StartsWith")!.GetValue(stringFilter)?.ToString() ?? string.Empty;
            parameters.Add(new SqlParameterValue(paramName, $"{raw}%"));
            return $"{textExpr} {op} {paramName}";
        });

        AddIfNotNull("EndsWith", () =>
        {
            var paramName = paramCtx.Next();
            var raw = type.GetProperty("EndsWith")!.GetValue(stringFilter)?.ToString() ?? string.Empty;
            parameters.Add(new SqlParameterValue(paramName, $"%{raw}"));
            return $"{textExpr} {op} {paramName}";
        });

        AddIfNotNull("Not", () =>
        {
            var notVal = type.GetProperty("Not")!.GetValue(stringFilter)!;
            return $"NOT ({BuildJsonStringFilterClause(column, segments, notVal, parameters, paramCtx)})";
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

    private static string WrapArrayPayload(object? value)
    {
        var raw = value?.ToString() ?? "null";
        return "[" + raw + "]";
    }

    private static string BuildArrayPayload(System.Collections.IEnumerable values)
    {
        var parts = new List<string>();
        foreach (var val in values)
        {
            parts.Add(val?.ToString() ?? "null");
        }

        return "[" + string.Join(",", parts) + "]";
    }

    private List<OrderTerm> BuildOrderTerms(ModelMetadata meta, System.Collections.IEnumerable? orderBy, string tableAlias, ref AliasContext aliasCtx, bool addTieBreaker = true)
    {
        var terms = new List<OrderTerm>();
        if (orderBy is null)
        {
            if (addTieBreaker)
            {
                AppendStableTieBreaker(meta, tableAlias, terms);
            }
            return terms;
        }

        foreach (var item in orderBy)
        {
            if (item is null)
            {
                continue;
            }

            var type = item.GetType();
            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var value = prop.GetValue(item);
                if (value is null)
                {
                    continue;
                }

                var field = meta.Fields.FirstOrDefault(f => string.Equals(f.Name, prop.Name, StringComparison.OrdinalIgnoreCase));
                if (field is null)
                {
                    continue;
                }

                if (field.Kind == FieldKind.Scalar)
                {
                    var direction = ResolveSortDirection(value);
                    terms.Add(new OrderTerm($"\"{tableAlias}\".{QuoteIdentifier(field.Name)}", direction));
                }
                else
                {
                    // Relation ordering is not expanded here; fallback to stable ordering to avoid SQL errors.
                }
            }
        }

        if (addTieBreaker)
        {
            AppendStableTieBreaker(meta, tableAlias, terms);
        }
        return terms;

    }

    private static string ResolveSortDirection(object value)
    {
        var order = value.ToString();
        return string.Equals(order, "Desc", StringComparison.OrdinalIgnoreCase) ? "DESC" : "ASC";
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

    private static bool IsSimpleValue(object value)
    {
        return value is string || value.GetType().IsValueType || value is Guid;
    }

    private static bool IsDefaultValue(object value)
    {
        var type = value.GetType();
        if (!type.IsValueType)
        {
            return false;
        }

        return Equals(value, Activator.CreateInstance(type));
    }

    private static List<OrderTerm> InvertOrderTerms(List<OrderTerm> terms)
    {
        return terms.Select(t => new OrderTerm(t.Expression, t.Direction.Equals("DESC", StringComparison.OrdinalIgnoreCase) ? "ASC" : "DESC")).ToList();
    }

    private static string BuildOrderSql(List<OrderTerm> terms, ModelMetadata meta, string tableAlias, bool addTieBreaker = true)
    {
        if (terms.Count == 0)
        {
            if (addTieBreaker)
            {
                AppendStableTieBreaker(meta, tableAlias, terms);
            }
        }
        return terms.Count == 0 ? string.Empty : string.Join(", ", terms.Select(t => $"{t.Expression} {t.Direction}"));
    }

    private static string BuildCursorPredicate(ModelMetadata meta, object? cursorObj, List<OrderTerm> orderTerms, string tableAlias, List<SqlParameterValue> parameters, ParameterContext paramCtx)
    {
        if (cursorObj is null)
        {
            return string.Empty;
        }

        if (meta.PrimaryKey is null || meta.PrimaryKey.Fields.Count == 0)
        {
            throw new InvalidOperationException($"Cursor pagination requires a primary key on '{meta.Name}'.");
        }

        // Only support single-field cursor for now.
        var pkField = meta.PrimaryKey.Fields[0];
        var prop = cursorObj.GetType().GetProperty(pkField, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        var value = prop?.GetValue(cursorObj);
        if (value is null)
        {
            return string.Empty;
        }

        var direction = orderTerms.FirstOrDefault()?.Direction ?? "ASC";
        var comparison = direction.Equals("DESC", StringComparison.OrdinalIgnoreCase) ? "<" : ">";
        var paramName = paramCtx.Next();
        parameters.Add(new SqlParameterValue(paramName, value));
        return $"\"{tableAlias}\".{QuoteIdentifier(pkField)} {comparison} {paramName}";
    }

    private static string QuoteIdentifier(string identifier)
    {
        var folded = _preserveIdentifierCasing ? identifier : identifier.ToLowerInvariant();
        return $"\"{folded}\"";
    }

    private sealed record RelationMapping(IReadOnlyList<string> ParentColumns, IReadOnlyList<string> ChildColumns);

    private sealed record OrderTerm(string Expression, string Direction);

    private sealed class ParameterContext
    {
        private int _counter;
        public string Next() => $"@p{++_counter}";
    }

    private sealed class AliasContext
    {
        private int _relationCounter;
        private int _orderCounter;

        public string NextRelationAlias() => $"rf{++_relationCounter}";
        public string NextOrderAlias() => $"ob{++_orderCounter}";
    }

    private void EnforceNestingDepth(ModelMetadata meta, object? selectObj, object? omitObj, object? includeObj, int depth = 0)
    {
        if (depth > _maxNestingDepth)
        {
            throw new NotSupportedException($"Select/Include/Omit nesting depth exceeds configured maximum of {_maxNestingDepth}.");
        }

        if (selectObj is not null)
        {
            foreach (var relation in meta.Fields.Where(f => f.Kind == FieldKind.Relation))
            {
                var child = selectObj.GetType().GetProperty(relation.Name, BindingFlags.Public | BindingFlags.Instance)?.GetValue(selectObj);
                if (child is not null)
                {
                    var childMeta = GetModelMetadata(relation.ClrType);
                    EnforceNestingDepth(childMeta, child, null, null, depth + 1);
                }
            }
        }

        if (includeObj is not null)
        {
            var includeSelect = GetProperty(includeObj, "Select");
            var includeOmit = GetProperty(includeObj, "Omit");
            EnsureSelectIncludeOmitExclusivity(includeSelect, null, includeOmit);
            if (includeSelect is not null || includeOmit is not null)
            {
                EnforceNestingDepth(meta, includeSelect, includeOmit, null, depth);
            }

            foreach (var relation in meta.Fields.Where(f => f.Kind == FieldKind.Relation))
            {
                var childInclude = includeObj.GetType().GetProperty(relation.Name, BindingFlags.Public | BindingFlags.Instance)?.GetValue(includeObj);
                if (childInclude is null)
                {
                    continue;
                }

                var childMeta = GetModelMetadata(relation.ClrType);
                EnforceNestingDepth(childMeta, null, null, childInclude, depth + 1);
            }
        }
    }
}
