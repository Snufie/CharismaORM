namespace Charisma.Generator;

/// <summary>
/// Configuration for Phase 1 code generation.
/// </summary>
public sealed class GeneratorOptions
{
    /// <summary>
    /// Root namespace for all generated code.
    /// </summary>
    public required string RootNamespace { get; init; }

    /// <summary>
    /// Generator version string embedded into generated headers.
    /// </summary>
    public required string GeneratorVersion { get; init; }

    /// <summary>
    /// Output directory where the Generated/ folder is written.
    /// </summary>
    public required string OutputDirectory { get; init; }
}
