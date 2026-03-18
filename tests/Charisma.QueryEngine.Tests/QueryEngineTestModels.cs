using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Planning;
using Charisma.Schema;

namespace Charisma.QueryEngine.Tests;

internal static class QueryEngineTestModels
{
    public static ModelMetadata BuildThingModel()
    {
        return new ModelMetadata(
            "Thing",
            new PrimaryKeyMetadata(new[] { "Id" }),
            Array.Empty<UniqueConstraintMetadata>(),
            Array.Empty<IndexMetadata>(),
            Array.Empty<ForeignKeyMetadata>(),
            new[]
            {
                new FieldMetadata("Id", "Guid", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("Name", "String", false, false, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Score", "Int", false, true, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Data", "Json", false, true, FieldKind.Scalar, false, false, false, null)
            });
    }

    public static ModelMetadata BuildMembershipModel()
    {
        return new ModelMetadata(
            "Membership",
            new PrimaryKeyMetadata(new[] { "UserId", "TeamId" }),
            new[]
            {
                new UniqueConstraintMetadata(new[] { "Email" }, null)
            },
            Array.Empty<IndexMetadata>(),
            Array.Empty<ForeignKeyMetadata>(),
            new[]
            {
                new FieldMetadata("UserId", "Guid", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("TeamId", "Guid", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("Email", "String", false, false, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Role", "String", false, false, FieldKind.Scalar, false, false, false, null)
            });
    }

    public static (ModelMetadata Parent, ModelMetadata Child) BuildParentChildModels()
    {
        var parent = new ModelMetadata(
            "Parent",
            new PrimaryKeyMetadata(new[] { "Id" }),
            Array.Empty<UniqueConstraintMetadata>(),
            Array.Empty<IndexMetadata>(),
            Array.Empty<ForeignKeyMetadata>(),
            new[]
            {
                new FieldMetadata("Id", "Guid", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("Name", "String", false, false, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Children", "Child", true, false, FieldKind.Relation, false, false, false, null)
            });

        var child = new ModelMetadata(
            "Child",
            new PrimaryKeyMetadata(new[] { "Id" }),
            Array.Empty<UniqueConstraintMetadata>(),
            Array.Empty<IndexMetadata>(),
            new[]
            {
                new ForeignKeyMetadata(new[] { "ParentId" }, "Parent", new[] { "Id" }, null, OnDeleteBehavior.Restrict)
            },
            new[]
            {
                new FieldMetadata("Id", "Guid", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("ParentId", "Guid", false, false, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Value", "String", false, true, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Parent", "Parent", false, false, FieldKind.Relation, false, false, false, null)
            });

        return (parent, child);
    }

    public static PostgresSqlPlanner BuildPlanner(params ModelMetadata[] models)
    {
        var registry = models.ToDictionary(m => m.Name, m => m, StringComparer.Ordinal);
        return new PostgresSqlPlanner(registry, preserveIdentifierCasing: false);
    }
}
