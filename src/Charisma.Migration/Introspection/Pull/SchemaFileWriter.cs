using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;

namespace Charisma.Migration;

/// <summary>
/// Writes the canonical schema representation to disk, ensuring datasource and generator blocks are preserved.
/// </summary>
public sealed class SchemaFileWriter : ISchemaWriter
{
    public async Task<SchemaWriteResult> WriteAsync(CharismaSchema schema, string schemaPath, bool overwrite, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(schemaPath);

        var content = schema.CanonicalText;

        if (File.Exists(schemaPath))
        {
            var existing = await File.ReadAllTextAsync(schemaPath, cancellationToken).ConfigureAwait(false);
            if (string.Equals(existing, content, StringComparison.Ordinal))
            {
                return new SchemaWriteResult(Written: false, Skipped: true, Reason: "Content unchanged");
            }

            if (!overwrite)
            {
                return new SchemaWriteResult(Written: false, Skipped: true, Reason: "File exists and overwrite is disabled");
            }
        }

        var directory = Path.GetDirectoryName(schemaPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(schemaPath, content, cancellationToken).ConfigureAwait(false);
        return new SchemaWriteResult(Written: true, Skipped: false, Reason: null);
    }
}
