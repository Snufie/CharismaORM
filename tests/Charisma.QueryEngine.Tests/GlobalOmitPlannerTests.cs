using System;
using System.Collections.Generic;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Charisma.QueryEngine.Planning;
using Xunit;

namespace Charisma.QueryEngine.Tests;

public class GlobalOmitPlannerTests
{
    private static ModelMetadata BuildUserMeta()
    {
        return new ModelMetadata(
            "User",
            new PrimaryKeyMetadata(new[] { "Id" }),
            Array.Empty<UniqueConstraintMetadata>(),
            Array.Empty<IndexMetadata>(),
            Array.Empty<ForeignKeyMetadata>(),
            new[]
            {
                new FieldMetadata("Id", "int", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("Secret", "string", false, true, FieldKind.Scalar, false, false, false, null),
                new FieldMetadata("Name", "string", false, true, FieldKind.Scalar, false, false, false, null)
            });
    }

    private static PostgresSqlPlanner BuildPlanner(IReadOnlyDictionary<string, object?>? globalOmit)
    {
        var meta = BuildUserMeta();
        var registry = new Dictionary<string, ModelMetadata>(StringComparer.Ordinal) { { meta.Name, meta } };
        return new PostgresSqlPlanner(registry, preserveIdentifierCasing: false, maxNestingDepth: 8, globalOmit: globalOmit);
    }

    [Fact]
    public void Global_omit_applies_when_no_overrides()
    {
        var planner = BuildPlanner(new Dictionary<string, object?> { { "User", new { Secret = (bool?)true } } });
        var plan = planner.Plan(new FindManyQueryModel("User", new { Where = new { } }));

        Assert.DoesNotContain("secret", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("id", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Omit_false_overrides_global_default()
    {
        var planner = BuildPlanner(new Dictionary<string, object?> { { "User", new { Secret = (bool?)true } } });
        var plan = planner.Plan(new FindManyQueryModel("User", new { Where = new { }, Omit = new { Secret = (bool?)false } }));

        Assert.Contains("secret", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Select_overrides_global_omit()
    {
        var planner = BuildPlanner(new Dictionary<string, object?> { { "User", new { Secret = (bool?)true } } });
        var plan = planner.Plan(new FindManyQueryModel("User", new { Where = new { }, Select = new { Secret = true } }));

        Assert.Contains("secret", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("name", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }
}
