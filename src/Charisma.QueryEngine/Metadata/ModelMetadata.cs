using Charisma.Schema;

namespace Charisma.QueryEngine.Metadata;

public enum FieldKind
{
    Scalar,
    Relation
}

/// <summary>
/// Default value metadata for scalar fields.
/// </summary>
public sealed record DefaultValueMetadata(DefaultValueKind Kind, string? Value);

/// <summary>
/// Describes a model field including shape, nullability, and constraints.
/// </summary>
public sealed record FieldMetadata(
    string Name,
    string ClrType,
    bool IsList,
    bool IsNullable,
    FieldKind Kind,
    bool IsPrimaryKey,
    bool IsUnique,
    bool IsUpdatedAt,
    DefaultValueMetadata? DefaultValue);

public sealed record PrimaryKeyMetadata(IReadOnlyList<string> Fields);

/// <summary>
/// Unique constraint metadata for a model.
/// </summary>
public sealed record UniqueConstraintMetadata(IReadOnlyList<string> Fields, string? Name);

/// <summary>
/// Index metadata (unique or non-unique) for a model.
/// </summary>
public sealed record IndexMetadata(IReadOnlyList<string> Fields, bool IsUnique, string? Name);

/// <summary>
/// Foreign key metadata describing local/foreign fields and delete behavior.
/// </summary>
public sealed record ForeignKeyMetadata(
    IReadOnlyList<string> LocalFields,
    string PrincipalModel,
    IReadOnlyList<string> PrincipalFields,
    string? RelationName,
    OnDeleteBehavior OnDelete);

/// <summary>
/// Aggregated metadata for a model consumed by the QueryEngine at runtime.
/// </summary>
public sealed record ModelMetadata(
    string Name,
    PrimaryKeyMetadata? PrimaryKey,
    IReadOnlyList<UniqueConstraintMetadata> UniqueConstraints,
    IReadOnlyList<IndexMetadata> Indexes,
    IReadOnlyList<ForeignKeyMetadata> ForeignKeys,
    IReadOnlyList<FieldMetadata> Fields);
