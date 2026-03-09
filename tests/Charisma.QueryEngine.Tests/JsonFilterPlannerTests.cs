using System;
using System.Collections.Generic;
using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Charisma.QueryEngine.Planning;
using Xunit;

namespace Charisma.QueryEngine.Tests;

public class JsonFilterPlannerTests
{
    private static ModelMetadata BuildModel()
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
                new FieldMetadata("Data", "Json", false, true, FieldKind.Scalar, false, false, false, null)
            });
    }

    private static PostgresSqlPlanner BuildPlanner()
    {
        var meta = BuildModel();
        var registry = new Dictionary<string, ModelMetadata>(StringComparer.Ordinal) { { meta.Name, meta } };
        return new PostgresSqlPlanner(registry, preserveIdentifierCasing: false);
    }

    [Fact]
    public void Json_path_string_filter_uses_text_projection_and_like()
    {
        var planner = BuildPlanner();
        var where = new
        {
            Data = new
            {
                path = new
                {
                    path = new[] { "info", "name" },
                    stringFilter = new { Contains = "robo", Mode = "Insensitive" }
                }
            }
        };

        var plan = planner.Plan(new FindManyQueryModel("Thing", new { Where = where }));

        Assert.Contains("#>> '{\"info\",\"name\"}'", plan.CommandText, StringComparison.Ordinal);
        Assert.Contains("ILIKE", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Json_array_hasSome_generates_array_predicate()
    {
        var planner = BuildPlanner();
        var where = new
        {
            Data = new
            {
                array_contains = new
                {
                    HasSome = new[] { "{\"foo\":1}" }
                }
            }
        };

        var plan = planner.Plan(new FindManyQueryModel("Thing", new { Where = where }));

        Assert.Contains("jsonb_typeof(\"t0\".\"Data\")", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("@>", plan.CommandText, StringComparison.Ordinal);
    }
}
