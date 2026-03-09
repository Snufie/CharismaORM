using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;
using Npgsql;

namespace Charisma.Migration.Introspection.Push.Postgres;

/// <summary>
/// Applies a Charisma schema to Postgres by emitting and executing DDL.
/// Currently assumes a fresh database or idempotent creation (CREATE IF NOT EXISTS for types/tables).
/// </summary>
public sealed class PostgresSchemaPusher : ISchemaPusher
{
    private readonly PostgresPushOptions _options;

    public PostgresSchemaPusher(PostgresPushOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task PushAsync(CharismaSchema schema, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        cancellationToken.ThrowIfCancellationRequested();

        var commands = GenerateDdl(schema);

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        foreach (var cmdText in commands)
        {
            await using var cmd = new NpgsqlCommand(cmdText, conn, tx);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static IEnumerable<string> GenerateDdl(CharismaSchema schema)
    {
        // 1) Enums
        foreach (var enumDef in schema.Enums.Values)
        {
            yield return PostgresSchemaPusherHelpers.BuildEnum(enumDef);
        }

        // 2) Tables
        foreach (var model in schema.Models.Values)
        {
            yield return PostgresSchemaPusherHelpers.BuildTable(model, schema.Enums);
        }

        // 3) Constraints after all tables exist (FKs, uniques/indexes that weren't inlined)
        foreach (var model in schema.Models.Values)
        {
            foreach (var fk in BuildForeignKeys(model))
            {
                yield return fk;
            }

            foreach (var idx in BuildIndexes(model))
            {
                yield return idx;
            }
        }
    }

    private static IEnumerable<string> BuildForeignKeys(ModelDefinition model)
    {
        foreach (var rel in model.Fields.OfType<RelationFieldDefinition>())
        {
            if (rel.RelationInfo is null) continue;
            if (rel.RelationInfo.IsCollection) continue;

            yield return PostgresSchemaPusherHelpers.BuildForeignKey(model.Name, rel.RelationInfo);
        }
    }

    private static IEnumerable<string> BuildIndexes(ModelDefinition model)
    {
        foreach (var idx in model.Indexes)
        {
            yield return PostgresSchemaPusherHelpers.BuildIndex(model.Name, idx);
        }
    }
}
