using Charisma.QueryEngine.Metadata;
using Charisma.QueryEngine.Model;
using Charisma.QueryEngine.Planning;

namespace Charisma.QueryEngine.Tests;

public class QueryPlannerSecurityTests
{
    [Fact]
    public void PlannerInstances_DoNotShareIdentifierCasingSettings()
    {
        var model = new ModelMetadata(
            "RobotRecord",
            new PrimaryKeyMetadata(new[] { "RobotID" }),
            Array.Empty<UniqueConstraintMetadata>(),
            Array.Empty<IndexMetadata>(),
            Array.Empty<ForeignKeyMetadata>(),
            new[]
            {
                new FieldMetadata("RobotID", "Guid", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("DisplayName", "String", false, false, FieldKind.Scalar, false, false, false, null)
            });

        var registry = new Dictionary<string, ModelMetadata>(StringComparer.Ordinal)
        {
            [model.Name] = model
        };

        var preservingPlanner = new PostgresSqlPlanner(registry, preserveIdentifierCasing: true);
        var foldingPlanner = new PostgresSqlPlanner(registry, preserveIdentifierCasing: false);

        var preservingPlan = preservingPlanner.Plan(new FindManyQueryModel(model.Name, new { Where = new { } }));
        var foldingPlan = foldingPlanner.Plan(new FindManyQueryModel(model.Name, new { Where = new { } }));

        Assert.Contains("\"RobotRecord\"", preservingPlan.CommandText, StringComparison.Ordinal);
        Assert.Contains("\"robotrecord\"", foldingPlan.CommandText, StringComparison.Ordinal);
        Assert.DoesNotContain("\"RobotRecord\"", foldingPlan.CommandText, StringComparison.Ordinal);
    }

    [Fact]
    public void ScalarFilterPayload_IsParameterized_NotInterpolated()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());
        var payload = "'; DROP TABLE users; --";

        var plan = planner.Plan(new FindManyQueryModel("Thing", new
        {
            Where = new
            {
                Name = new { Contains = payload }
            }
        }));

        Assert.DoesNotContain(payload, plan.CommandText, StringComparison.Ordinal);
        Assert.Contains("@p1", plan.CommandText, StringComparison.Ordinal);
        Assert.Contains(plan.Parameters, p => Equals(p.Value, $"%{payload}%"));
    }

    [Fact]
    public void JsonStringFilterPayload_IsParameterized_NotInterpolated()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());
        var payload = "x' OR 1=1 --";

        var plan = planner.Plan(new FindManyQueryModel("Thing", new
        {
            Where = new
            {
                Data = new
                {
                    path = new
                    {
                        path = new[] { "meta", "name" },
                        stringFilter = new { Contains = payload, Mode = "Insensitive" }
                    }
                }
            }
        }));

        Assert.DoesNotContain(payload, plan.CommandText, StringComparison.Ordinal);
        Assert.Contains("@p1", plan.CommandText, StringComparison.Ordinal);
        Assert.Contains(plan.Parameters, p => Equals(p.Value, $"%{payload}%"));
    }

    [Fact]
    public void QuoteIdentifier_EscapesEmbeddedDoubleQuotes()
    {
        var dangerousModel = new ModelMetadata(
            "Thing\"; DROP TABLE x; --",
            new PrimaryKeyMetadata(new[] { "Id\"; DROP TABLE y; --" }),
            Array.Empty<UniqueConstraintMetadata>(),
            Array.Empty<IndexMetadata>(),
            Array.Empty<ForeignKeyMetadata>(),
            new[]
            {
                new FieldMetadata("Id\"; DROP TABLE y; --", "Guid", false, false, FieldKind.Scalar, true, false, false, null),
                new FieldMetadata("Name\"; DROP TABLE z; --", "String", false, false, FieldKind.Scalar, false, false, false, null)
            });

        var planner = QueryEngineTestModels.BuildPlanner(dangerousModel);
        var plan = planner.Plan(new FindManyQueryModel(dangerousModel.Name, new { Where = new { } }));

        Assert.Contains("\"thing\"\"; drop table x; --\"", plan.CommandText, StringComparison.Ordinal);
        Assert.Contains("\"name\"\"; drop table z; --\"", plan.CommandText, StringComparison.Ordinal);
    }
}
