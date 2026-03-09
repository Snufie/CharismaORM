using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;

namespace Charisma.Migration;

/// <summary>
/// Persists a schema to the canonical schema file on disk.
/// </summary>
public interface ISchemaWriter
{
    /// <summary>
    /// Writes the provided schema to the given path.
    /// </summary>
    Task<SchemaWriteResult> WriteAsync(CharismaSchema schema, string schemaPath, bool overwrite, CancellationToken cancellationToken = default);
}

/// <summary>
/// Result of attempting to write a schema file.
/// </summary>
public sealed record SchemaWriteResult(bool Written, bool Skipped, string? Reason);
