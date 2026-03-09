using System;
using System.Threading;
using System.Threading.Tasks;

namespace Charisma.Migration;

/// <summary>
/// Coordinates pulling schema from a data source and writing it to disk.
/// </summary>
public sealed class SchemaPullService
{
    private readonly ISchemaIntrospector _introspector;
    private readonly ISchemaWriter _writer;

    public SchemaPullService(ISchemaIntrospector introspector, ISchemaWriter writer)
    {
        _introspector = introspector ?? throw new ArgumentNullException(nameof(introspector));
        _writer = writer ?? throw new ArgumentNullException(nameof(writer));
    }

    /// <summary>
    /// Introspects and writes the schema file. Caller controls overwrite behavior/confirmation.
    /// </summary>
    public async Task<SchemaWriteResult> PullAsync(string schemaPath, bool overwrite, CancellationToken cancellationToken = default)
    {
        var schema = await _introspector.IntrospectAsync(cancellationToken).ConfigureAwait(false);
        return await _writer.WriteAsync(schema, schemaPath, overwrite, cancellationToken).ConfigureAwait(false);
    }
}
