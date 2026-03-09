using System;
using System.Collections.Generic;
using System.Reflection;
using Charisma.QueryEngine.Execution;
using Charisma.QueryEngine.Metadata;

namespace Charisma.Runtime;

/// <summary>
/// Configuration for CharismaRuntime construction.
/// </summary>
public sealed class CharismaRuntimeOptions
{
    /// <summary>
    /// Database connection string (required).
    /// </summary>
    public required string ConnectionString { get; init; }

    /// <summary>
    /// Optional connection provider override. If supplied, ConnectionString is only used for metadata defaults.
    /// </summary>
    public IConnectionProvider? ConnectionProvider { get; init; }

    /// <summary>
    /// Target database provider.
    /// </summary>
    public ProviderOptions Provider { get; init; } = ProviderOptions.PostgreSQL;

    /// <summary>
    /// Root namespace of generated code (used to resolve metadata and POCOs).
    /// </summary>
    public required string RootNamespace { get; init; }

    /// <summary>
    /// Optional explicit assembly containing generated code. Defaults to entry assembly.
    /// </summary>
    public Assembly? GeneratedAssembly { get; set; }

    /// <summary>
    /// Optional metadata registry override. When not provided, the runtime will load ModelMetadataRegistry from the generated assembly.
    /// </summary>
    public IReadOnlyDictionary<string, ModelMetadata>? MetadataRegistry { get; init; }

    /// <summary>
    /// Optional custom model type resolver.
    /// </summary>
    public Func<string, Type>? ModelTypeResolver { get; init; }

    /// <summary>
    /// When false (default), identifiers are folded to lowercase to match Postgres defaults. Set true to preserve mixed-case identifiers.
    /// </summary>
    public bool PreserveIdentifierCasing { get; init; }

    /// <summary>
    /// Maximum allowed nesting depth for select/include/omit graphs (relations traversed).
    /// </summary>
    public int MaxNestingDepth { get; init; } = 12;

    /// <summary>
    /// Optional default omit masks keyed by model name; applied when no select/omit is provided.
    /// </summary>
    public IReadOnlyDictionary<string, object?>? GlobalOmit { get; init; }
}
