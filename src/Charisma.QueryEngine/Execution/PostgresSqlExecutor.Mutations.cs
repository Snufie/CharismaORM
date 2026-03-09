using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    private async Task<object?> HydrateSingleWithIncludeAsync(ModelMetadata meta, object baseRow, object? select, object? include, NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct)
    {
        if (include is null)
        {
            return baseRow;
        }

        var pk = ExtractPrimaryKeyValues(meta, baseRow);
        var args = new { Where = (object)pk, Include = include, Select = select };

        var query = new FindUniqueQueryModel(meta.Name, args);
        return await ExecutePlannedQueryAsync(query, SqlPlanKind.QuerySingle, new SqlExecutionContext(conn, tx), ct).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<object>> HydrateManyWithIncludeAsync(ModelMetadata meta, IReadOnlyList<object> baseRows, object? select, object? include, NpgsqlConnection conn, NpgsqlTransaction? tx, CancellationToken ct)
    {
        if (include is null || baseRows.Count == 0)
        {
            return baseRows;
        }

        if (meta.PrimaryKey is not { Fields.Count: > 0 })
        {
            throw new InvalidOperationException($"Model '{meta.Name}' is missing primary key metadata required for include hydration.");
        }

        var pkFilters = new List<Dictionary<string, object?>>(baseRows.Count);
        var orderKeys = new List<CompositeKey>(baseRows.Count);

        foreach (var row in baseRows)
        {
            var pkValues = ExtractPrimaryKeyValues(meta, row);
            pkFilters.Add(pkValues);
            orderKeys.Add(new CompositeKey(meta.PrimaryKey.Fields.Select(f => pkValues[f]).ToList()));
        }

        var where = pkFilters.Count == 1
            ? (object)pkFilters[0]
            : new { OR = pkFilters };

        var args = new { Where = where, Include = include, Select = select };

        var query = new FindManyQueryModel(meta.Name, args);
        var result = await ExecutePlannedQueryAsync(query, SqlPlanKind.QueryMany, new SqlExecutionContext(conn, tx), ct).ConfigureAwait(false);
        if (result is not IReadOnlyList<object> hydrated)
        {
            throw new InvalidOperationException("Include hydration expected a list result.");
        }

        var lookup = new Dictionary<CompositeKey, object>(CompositeKeyComparer.Instance);
        foreach (var item in hydrated)
        {
            var pkValues = ExtractPrimaryKeyValues(meta, item);
            var key = new CompositeKey(meta.PrimaryKey.Fields.Select(f => pkValues[f]).ToList());
            lookup[key] = item;
        }

        var ordered = new List<object>(baseRows.Count);
        foreach (var key in orderKeys)
        {
            if (lookup.TryGetValue(key, out var materialized))
            {
                ordered.Add(materialized);
            }
        }

        return ordered.AsReadOnly();
    }

    private static List<string> EnsurePrimaryKeySelected(ModelMetadata meta, IReadOnlyList<string> columns)
    {
        var list = columns.ToList();
        if (meta.PrimaryKey is { Fields.Count: > 0 })
        {
            foreach (var pk in meta.PrimaryKey.Fields)
            {
                if (!list.Contains(pk, StringComparer.Ordinal))
                {
                    list.Add(pk);
                }
            }
        }

        return list;
    }

    /// <summary>
    /// Executes a single-row insert with scalar projections.
    /// </summary>
    private async Task<object?> ExecuteCreateAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var data = GetRequiredProperty(args, "Data");
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");

        var relationPayloads = ExtractRelationPayloads(meta, data);
        var returningColumns = EnsurePrimaryKeySelected(meta, BuildSelectColumns(meta, select));

        if (relationPayloads.Count == 0 && include is null)
        {
            var (columns, paramList) = BuildInsertColumns(meta, data);

            var sql = new StringBuilder();
            sql.Append("INSERT INTO ").Append(QuoteIdentifier(meta.Name)).Append(' ');
            if (columns.Count == 0)
            {
                sql.Append("DEFAULT VALUES");
            }
            else
            {
                sql.Append('(')
                    .Append(string.Join(", ", columns.Select(QuoteIdentifier)))
                    .Append(") VALUES (")
                    .Append(string.Join(", ", paramList.Select(p => p.ParameterName)))
                    .Append(')');
            }
            sql.Append(" RETURNING ")
                .Append(string.Join(", ", returningColumns.Select(QuoteIdentifier)));

            var parameters = paramList.ToList();
            var results = conn is null
                ? await ExecuteReaderAsync(meta, sql.ToString(), parameters, ct).ConfigureAwait(false)
                : await ExecuteReaderAsync(meta, sql.ToString(), parameters, conn, tx, ct).ConfigureAwait(false);
            if (results.Count == 0)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.Create));
            }
            return results[0];
        }

        return await WithTransactionAsync(async (c, transaction) =>
        {
            var filteredRelations = relationPayloads;
            var parentFkOverrides = new Dictionary<string, object?>(StringComparer.Ordinal);

            if (relationPayloads.Count > 0)
            {
                (filteredRelations, parentFkOverrides, _) = await ResolveParentOwnedFkOverridesForCreateAsync(meta, relationPayloads, c, transaction, ct).ConfigureAwait(false);
            }

            var (columns, paramList) = BuildInsertColumns(meta, data, overrideScalars: parentFkOverrides);
            var sql = new StringBuilder();
            sql.Append("INSERT INTO ").Append(QuoteIdentifier(meta.Name)).Append(' ');
            if (columns.Count == 0)
            {
                sql.Append("DEFAULT VALUES");
            }
            else
            {
                sql.Append('(')
                    .Append(string.Join(", ", columns.Select(QuoteIdentifier)))
                    .Append(") VALUES (")
                    .Append(string.Join(", ", paramList.Select(p => p.ParameterName)))
                    .Append(')');
            }
            sql.Append(" RETURNING ")
                .Append(string.Join(", ", returningColumns.Select(QuoteIdentifier)));

            var parameters = paramList.ToList();
            var results = await ExecuteReaderAsync(meta, sql.ToString(), parameters, c, transaction, ct).ConfigureAwait(false);
            if (results.Count == 0)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.Create));
            }

            var parent = results[0];
            if (filteredRelations.Count > 0)
            {
                await ExecuteRelationWritesAsync(meta, filteredRelations, parent, c, transaction, ct).ConfigureAwait(false);
            }

            var hydrated = await HydrateSingleWithIncludeAsync(meta, parent, select, include, c, transaction, ct).ConfigureAwait(false);
            return hydrated ?? parent;
        }, ct, conn, tx).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes sequential inserts for a batch of records.
    /// </summary>
    private async Task<object?> ExecuteCreateManyAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        if (query is CreateManyQueryModel { ReturnRecords: false })
        {
            var count = await ExecuteCreateManyNonQueryAsync(query, context, ct).ConfigureAwait(false);
            return count;
        }

        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var dataList = GetRequiredProperty(args, "Data") as System.Collections.IEnumerable
                       ?? throw new InvalidOperationException("Data for CreateMany must be an enumerable.");
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");
        EnsureIncludeNotProvided(include);
        var skipDuplicates = GetProperty(args, "SkipDuplicates") as bool? == true;
        var returningColumns = EnsurePrimaryKeySelected(meta, BuildSelectColumns(meta, select));

        async Task<object?> ExecuteBatch(NpgsqlConnection c, NpgsqlTransaction? transaction)
        {
            var results = new List<object>();
            foreach (var item in dataList)
            {
                EnsureNoRelationWrites(meta, item!);
                var (columns, paramList) = BuildInsertColumns(meta, item!);
                var sql = new StringBuilder();
                sql.Append("INSERT INTO ").Append(QuoteIdentifier(meta.Name)).Append(' ');
                if (columns.Count == 0)
                {
                    sql.Append("DEFAULT VALUES");
                }
                else
                {
                    sql.Append('(')
                        .Append(string.Join(", ", columns.Select(QuoteIdentifier)))
                        .Append(") VALUES (")
                        .Append(string.Join(", ", paramList.Select(p => p.ParameterName)))
                        .Append(')');
                }
                if (skipDuplicates)
                {
                    sql.Append(" ON CONFLICT DO NOTHING");
                }
                sql.Append(" RETURNING ")
                    .Append(string.Join(", ", returningColumns.Select(QuoteIdentifier)));

                var parameters = paramList.ToList();
                var row = await ExecuteReaderAsync(meta, sql.ToString(), parameters, c, transaction, ct).ConfigureAwait(false);
                if (row.Count == 0)
                {
                    if (skipDuplicates)
                    {
                        continue;
                    }
                    throw new VoidTouchException(meta.Name, nameof(QueryType.CreateMany));
                }
                results.Add(row[0]);
            }

            if (results.Count == 0 && !skipDuplicates)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.CreateMany));
            }

            var materialized = await HydrateManyWithIncludeAsync(meta, results.AsReadOnly(), select, include, c, transaction, ct).ConfigureAwait(false);
            return materialized;
        }

        if (conn is not null)
        {
            return await ExecuteBatch(conn, tx).ConfigureAwait(false);
        }

        return await WithTransactionAsync(ExecuteBatch, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes sequential inserts for a batch of records and returns affected count.
    /// </summary>
    private async Task<int> ExecuteCreateManyNonQueryAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var dataList = GetRequiredProperty(args, "Data") as System.Collections.IEnumerable
                       ?? throw new InvalidOperationException("Data for CreateMany must be an enumerable.");
        var skipDuplicates = GetProperty(args, "SkipDuplicates") as bool? == true;
        EnsureIncludeNotProvided(GetProperty(args, "Include"));

        async Task<int> ExecuteBatch(NpgsqlConnection c, NpgsqlTransaction? transaction)
        {
            var affectedTotal = 0;
            foreach (var item in dataList)
            {
                EnsureNoRelationWrites(meta, item!);
                var (columns, paramList) = BuildInsertColumns(meta, item!);
                var sql = new StringBuilder();
                sql.Append("INSERT INTO ").Append(QuoteIdentifier(meta.Name)).Append(' ');
                if (columns.Count == 0)
                {
                    sql.Append("DEFAULT VALUES");
                }
                else
                {
                    sql.Append('(')
                        .Append(string.Join(", ", columns.Select(QuoteIdentifier)))
                        .Append(") VALUES (")
                        .Append(string.Join(", ", paramList.Select(p => p.ParameterName)))
                        .Append(')');
                }
                if (skipDuplicates)
                {
                    sql.Append(" ON CONFLICT DO NOTHING");
                }

                var parameters = paramList.ToList();
                var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, c, transaction, ct, meta.Name, nameof(QueryType.CreateMany)).ConfigureAwait(false);
                affectedTotal += affected;
            }

            if (affectedTotal == 0 && !skipDuplicates)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.CreateMany));
            }

            return affectedTotal;
        }

        if (conn is not null)
        {
            return await ExecuteBatch(conn, tx).ConfigureAwait(false);
        }

        return await WithTransactionAsync(ExecuteBatch, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes an Update with a unique where predicate.
    /// </summary>
    private async Task<object?> ExecuteUpdateAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var data = GetRequiredProperty(args, "Data");
        var where = GetRequiredProperty(args, "Where");
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");

        var relationPayloads = ExtractRelationPayloads(meta, data);
        var returningColumns = EnsurePrimaryKeySelected(meta, BuildSelectColumns(meta, select));

        if (relationPayloads.Count == 0 && include is null)
        {
            var (setSql, parameters, paramCtx) = BuildUpdateSet(meta, data);
            var whereSql = BuildWhereUnique(meta, where, parameters, paramCtx, "t");

            var sql = new StringBuilder();
            sql.Append("UPDATE ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" SET ").Append(setSql);
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
            sql.Append(" RETURNING ").Append(string.Join(", ", returningColumns.Select(c => $"\"t\".{QuoteIdentifier(c)}")));

            var rows = conn is null
                ? await ExecuteReaderAsync(meta, sql.ToString(), parameters, ct).ConfigureAwait(false)
                : await ExecuteReaderAsync(meta, sql.ToString(), parameters, conn, tx, ct).ConfigureAwait(false);
            if (rows.Count == 0)
            {
                throw new RecordNotFoundException(meta.Name, nameof(QueryType.Update));
            }
            return rows[0];
        }

        return await WithTransactionAsync(async (c, transaction) =>
        {
            var (setSql, parameters, paramCtx) = BuildUpdateSet(meta, data, allowEmpty: true);
            var whereSql = BuildWhereUnique(meta, where, parameters, paramCtx, "t");

            if (string.IsNullOrWhiteSpace(setSql))
            {
                var selectSql = new StringBuilder();
                selectSql.Append("SELECT ").Append(string.Join(", ", returningColumns.Select(c => $"\"t\".{QuoteIdentifier(c)}")));
                selectSql.Append(" FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\"");
                if (!string.IsNullOrWhiteSpace(whereSql))
                {
                    selectSql.Append(" WHERE ").Append(whereSql);
                }

                var existingRows = await ExecuteReaderAsync(meta, selectSql.ToString(), parameters, c, transaction, ct).ConfigureAwait(false);
                if (existingRows.Count == 0)
                {
                    throw new RecordNotFoundException(meta.Name, nameof(QueryType.Update));
                }

                var parentExisting = existingRows[0];
                await ExecuteRelationWritesAsync(meta, relationPayloads, parentExisting, c, transaction, ct).ConfigureAwait(false);
                var hydratedExisting = await HydrateSingleWithIncludeAsync(meta, parentExisting, select, include, c, transaction, ct).ConfigureAwait(false);
                return hydratedExisting ?? parentExisting;
            }

            var sql = new StringBuilder();
            sql.Append("UPDATE ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" SET ").Append(setSql);
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
            sql.Append(" RETURNING ").Append(string.Join(", ", returningColumns.Select(c => $"\"t\".{QuoteIdentifier(c)}")));

            var rows = await ExecuteReaderAsync(meta, sql.ToString(), parameters, c, transaction, ct).ConfigureAwait(false);
            if (rows.Count == 0)
            {
                throw new RecordNotFoundException(meta.Name, nameof(QueryType.Update));
            }

            var parent = rows[0];
            await ExecuteRelationWritesAsync(meta, relationPayloads, parent, c, transaction, ct).ConfigureAwait(false);
            var hydrated = await HydrateSingleWithIncludeAsync(meta, parent, select, include, c, transaction, ct).ConfigureAwait(false);
            return hydrated ?? parent;
        }, ct, conn, tx).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes UpdateMany with optional filters and returns the affected rows when requested.
    /// </summary>
    private async Task<object?> ExecuteUpdateManyAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        if (query is UpdateManyQueryModel { ReturnRecords: false })
        {
            var count = await ExecuteUpdateManyNonQueryAsync(query, context, ct).ConfigureAwait(false);
            return count;
        }

        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var data = GetRequiredProperty(args, "Data");
        EnsureNoRelationWrites(meta, data);
        var where = GetProperty(args, "Where");
        var limit = GetProperty(args, "Limit") as int?;
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");
        EnsureIncludeNotProvided(include);

        var (setSql, parameters, paramCtx) = BuildUpdateSet(meta, data);
        var aliasCtx = new AliasContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t", ref aliasCtx);
        var returningColumns = EnsurePrimaryKeySelected(meta, BuildSelectColumns(meta, select));

        var sql = new StringBuilder();
        if (limit.HasValue)
        {
            sql.Append("WITH target AS (SELECT ctid FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\"");
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
            sql.Append(" LIMIT ").Append(limit.Value).Append(") ");
            sql.Append("UPDATE ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" SET ").Append(setSql);
            sql.Append(" WHERE \"t\".ctid IN (SELECT ctid FROM target)");
        }
        else
        {
            sql.Append("UPDATE ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" SET ").Append(setSql);
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
        }
        sql.Append(" RETURNING ").Append(string.Join(", ", returningColumns.Select(c => $"\"t\".{QuoteIdentifier(c)}")));

        async Task<object?> ExecBatch(NpgsqlConnection c, NpgsqlTransaction? transaction)
        {
            var rows = await ExecuteReaderAsync(meta, sql.ToString(), parameters, c, transaction, ct).ConfigureAwait(false);
            if (rows.Count == 0)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.UpdateMany));
            }
            var hydrated = await HydrateManyWithIncludeAsync(meta, rows.AsReadOnly(), select, include, c, transaction, ct).ConfigureAwait(false);
            return hydrated;
        }

        if (conn is not null)
        {
            return await ExecBatch(conn, tx).ConfigureAwait(false);
        }

        return await WithTransactionAsync(ExecBatch, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes UpdateMany without returning rows.
    /// </summary>
    private async Task<int> ExecuteUpdateManyNonQueryAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var data = GetRequiredProperty(args, "Data");
        EnsureNoRelationWrites(meta, data);
        var where = GetProperty(args, "Where");
        var limit = GetProperty(args, "Limit") as int?;
        var include = GetProperty(args, "Include");
        EnsureIncludeNotProvided(include);

        var (setSql, parameters, paramCtx) = BuildUpdateSet(meta, data);
        var aliasCtx = new AliasContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t", ref aliasCtx);

        var sql = new StringBuilder();
        if (limit.HasValue)
        {
            sql.Append("WITH target AS (SELECT ctid FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\"");
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
            sql.Append(" LIMIT ").Append(limit.Value).Append(") ");
            sql.Append("UPDATE ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" SET ").Append(setSql);
            sql.Append(" WHERE \"t\".ctid IN (SELECT ctid FROM target)");
        }
        else
        {
            sql.Append("UPDATE ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" SET ").Append(setSql);
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
        }

        async Task<int> ExecBatch(NpgsqlConnection c, NpgsqlTransaction? transaction)
        {
            var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, c, transaction, ct, meta.Name).ConfigureAwait(false);
            if (affected == 0)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.UpdateMany));
            }
            return affected;
        }

        if (conn is not null)
        {
            return await ExecBatch(conn, tx).ConfigureAwait(false);
        }

        return await WithTransactionAsync(ExecBatch, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes Delete by unique predicate and returns the deleted row projection.
    /// </summary>
    private async Task<object?> ExecuteDeleteAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetRequiredProperty(args, "Where");
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");
        EnsureIncludeNotProvided(include);
        var returningColumns = BuildSelectColumns(meta, select);

        var parameters = new List<NpgsqlParameter>();
        var paramCtx = new ParameterContext();
        var whereSql = BuildWhereUnique(meta, where, parameters, paramCtx, "t");

        var sql = new StringBuilder();
        sql.Append("DELETE FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\"");
        if (!string.IsNullOrWhiteSpace(whereSql))
        {
            sql.Append(" WHERE ").Append(whereSql);
        }
        sql.Append(" RETURNING ").Append(string.Join(", ", returningColumns.Select(c => $"\"t\".{QuoteIdentifier(c)}")));

        var rows = conn is null
            ? await ExecuteReaderAsync(meta, sql.ToString(), parameters, ct).ConfigureAwait(false)
            : await ExecuteReaderAsync(meta, sql.ToString(), parameters, conn, tx, ct).ConfigureAwait(false);
        if (rows.Count == 0)
        {
            throw new RecordNotFoundException(meta.Name, nameof(QueryType.Delete));
        }
        return rows[0];
    }

    /// <summary>
    /// Executes DeleteMany and returns the affected row count.
    /// </summary>
    private async Task<int> ExecuteDeleteManyNonQueryAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetProperty(args, "Where");
        var limit = GetProperty(args, "Limit") as int?;

        var parameters = new List<NpgsqlParameter>();
        var paramCtx = new ParameterContext();
        var aliasCtx = new AliasContext();
        var whereSql = BuildWhere(meta, where, parameters, paramCtx, "t", ref aliasCtx);

        var sql = new StringBuilder();
        if (limit.HasValue)
        {
            sql.Append("DELETE FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" WHERE \"t\".ctid IN (SELECT ctid FROM ")
                .Append(QuoteIdentifier(meta.Name)).Append(" AS \"t0\"");
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql.Replace("\"t\"", "\"t0\""));
            }
            sql.Append(" LIMIT ").Append(limit.Value).Append(')');
        }
        else
        {
            sql.Append("DELETE FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\"");
            if (!string.IsNullOrWhiteSpace(whereSql))
            {
                sql.Append(" WHERE ").Append(whereSql);
            }
        }

        async Task<int> ExecBatch(NpgsqlConnection c, NpgsqlTransaction? transaction)
        {
            var affected = await ExecuteNonQueryInternalAsync(sql.ToString(), parameters, c, transaction, ct, meta.Name).ConfigureAwait(false);
            if (affected == 0)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.DeleteMany));
            }
            return affected;
        }

        if (conn is not null)
        {
            return await ExecBatch(conn, tx).ConfigureAwait(false);
        }

        return await WithTransactionAsync(ExecBatch, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes Upsert using an update-first, insert-second strategy with RETURNING.
    /// </summary>
    private async Task<object?> ExecuteUpsertAsync(QueryModel query, SqlExecutionContext? context, CancellationToken ct)
    {
        var (conn, tx) = ExtractContext(context);
        var meta = GetModelMetadata(query.ModelName);
        var args = query.Args;
        var where = GetRequiredProperty(args, "Where");
        var create = GetRequiredProperty(args, "Create");
        var update = GetRequiredProperty(args, "Update");
        var select = GetProperty(args, "Select");
        var include = GetProperty(args, "Include");
        var returningColumns = BuildSelectColumns(meta, select);

        var createRelationPayloads = ExtractRelationPayloads(meta, create);
        var updateRelationPayloads = ExtractRelationPayloads(meta, update);

        var parameters = new List<NpgsqlParameter>();
        var paramCtx = new ParameterContext();
        var whereSql = BuildWhereUnique(meta, where, parameters, paramCtx, "t");

        return await WithTransactionAsync(async (c, transaction) =>
        {
            // Try update first
            var (setSql, updateParams, _) = BuildUpdateSet(meta, update, paramCtx, allowEmpty: true);
            parameters.AddRange(updateParams);

            if (string.IsNullOrWhiteSpace(setSql))
            {
                var selectSql = new StringBuilder();
                selectSql.Append("SELECT ").Append(string.Join(", ", returningColumns.Select(c => $"\"t\".{QuoteIdentifier(c)}")));
                selectSql.Append(" FROM ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\"");
                if (!string.IsNullOrWhiteSpace(whereSql))
                {
                    selectSql.Append(" WHERE ").Append(whereSql);
                }

                var existingRows = await ExecuteReaderAsync(meta, selectSql.ToString(), parameters, c, transaction, ct).ConfigureAwait(false);
                if (existingRows.Count > 0)
                {
                    var existing = existingRows[0];
                    await ExecuteRelationWritesAsync(meta, updateRelationPayloads, existing, c, transaction, ct).ConfigureAwait(false);
                    var hydratedExisting = await HydrateSingleWithIncludeAsync(meta, existing, select, include, c, transaction, ct).ConfigureAwait(false);
                    return hydratedExisting ?? existing;
                }
            }
            else
            {
                var updateSql = new StringBuilder();
                updateSql.Append("UPDATE ").Append(QuoteIdentifier(meta.Name)).Append(" AS \"t\" SET ").Append(setSql);
                if (!string.IsNullOrWhiteSpace(whereSql))
                {
                    updateSql.Append(" WHERE ").Append(whereSql);
                }
                updateSql.Append(" RETURNING ").Append(string.Join(", ", returningColumns.Select(c => $"\"t\".{QuoteIdentifier(c)}")));

                var rows = await ExecuteReaderAsync(meta, updateSql.ToString(), parameters, c, transaction, ct).ConfigureAwait(false);
                if (rows.Count > 0)
                {
                    var updated = rows[0];
                    await ExecuteRelationWritesAsync(meta, updateRelationPayloads, updated, c, transaction, ct).ConfigureAwait(false);
                    var hydratedUpdated = await HydrateSingleWithIncludeAsync(meta, updated, select, include, c, transaction, ct).ConfigureAwait(false);
                    return hydratedUpdated ?? updated;
                }
            }

            var (filteredCreateRelations, createOverrides, _) = await ResolveParentOwnedFkOverridesForCreateAsync(meta, createRelationPayloads, c, transaction, ct).ConfigureAwait(false);

            var (columns, insertParams) = BuildInsertColumns(meta, create, paramCtx, overrideScalars: createOverrides);
            var insertSql = new StringBuilder();
            insertSql.Append("INSERT INTO ").Append(QuoteIdentifier(meta.Name)).Append(' ');
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
            insertSql.Append(" RETURNING ")
                .Append(string.Join(", ", returningColumns.Select(QuoteIdentifier)));

            var insertRows = await ExecuteReaderAsync(meta, insertSql.ToString(), insertParams.ToList(), c, transaction, ct).ConfigureAwait(false);
            if (insertRows.Count == 0)
            {
                throw new VoidTouchException(meta.Name, nameof(QueryType.Upsert));
            }

            var created = insertRows[0];
            await ExecuteRelationWritesAsync(meta, filteredCreateRelations, created, c, transaction, ct).ConfigureAwait(false);
            var hydratedCreated = await HydrateSingleWithIncludeAsync(meta, created, select, include, c, transaction, ct).ConfigureAwait(false);
            return hydratedCreated ?? created;
        }, ct, conn, tx).ConfigureAwait(false);
    }
}
