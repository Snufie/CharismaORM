using System;
using System.Collections.Generic;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Charisma.QueryEngine.Planning;
using Charisma.Schema;
using Xunit;

namespace Charisma.QueryEngine.Tests;

public class PlannerBehaviorTests
{
    [Fact]
    public void Logical_operators_accept_single_objects()
    {
        var meta = BuildSimpleModel();
        var planner = BuildPlanner(meta);
        var where = new
        {
            AND = new { Name = "alpha" },
            OR = new { Value = "beta" }
        };

        var plan = planner.Plan(new FindManyQueryModel(meta.Name, new { Where = where }));

        Assert.Contains("AND", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OR", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void String_filter_mode_respects_sensitivity()
    {
        var meta = BuildSimpleModel();
        var planner = BuildPlanner(meta);
        var where = new
        {
            Name = new { Contains = "Robot", Mode = "Sensitive" }
        };

        var plan = planner.Plan(new FindManyQueryModel(meta.Name, new { Where = where }));

        Assert.Contains(" LIKE ", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ILIKE", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Omit_mask_excludes_columns()
    {
        var meta = BuildSimpleModel();
        var planner = BuildPlanner(meta);

        var plan = planner.Plan(new FindManyQueryModel(meta.Name, new { Omit = new { Value = true } }));

        Assert.DoesNotContain("\"value\"", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"name\"", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Include_accepts_dictionary_payload()
    {
        var (parent, child) = BuildParentChildModels();
        var planner = new PostgresSqlPlanner(new Dictionary<string, ModelMetadata>(StringComparer.Ordinal)
        {
            { parent.Name, parent },
            { child.Name, child }
        });

        var include = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["Children"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Select"] = new { Value = true }
            }
        };

        var plan = planner.Plan(new FindManyQueryModel(parent.Name, new { Include = include }));

        Assert.Contains("LEFT JOIN \"child\" AS \"t1\"", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"t1\".\"value\" AS \"t1__value\"", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    private static ModelMetadata BuildSimpleModel()
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
                new FieldMetadata("Value", "String", false, false, FieldKind.Scalar, false, false, false, null)
            });
    }

    private static (ModelMetadata Parent, ModelMetadata Child) BuildParentChildModels()
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
                new FieldMetadata("ParentId", "Guid", false, true, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Value", "String", false, false, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Parent", "Parent", false, false, FieldKind.Relation, false, false, false, null)
            });

        return (parent, child);
    }

    private static PostgresSqlPlanner BuildPlanner(ModelMetadata meta)
    {
        var registry = new Dictionary<string, ModelMetadata>(StringComparer.Ordinal)
        {
            { meta.Name, meta }
        };
        return new PostgresSqlPlanner(registry, preserveIdentifierCasing: false);
    }
}
