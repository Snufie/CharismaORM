using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Tests;

public class QueryPlannerJsonTests
{
    [Fact]
    public void JsonPath_BuildsExpectedProjectionSyntax()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());

        var plan = planner.Plan(new FindManyQueryModel("Thing", new
        {
            Where = new
            {
                Data = new
                {
                    path = new
                    {
                        path = new[] { "info", "name" },
                        stringFilter = new { Equals = "robot", Mode = "Sensitive" }
                    }
                }
            }
        }));

        Assert.Contains("#>> '{\"info\",\"name\"}'", plan.CommandText, StringComparison.Ordinal);
        Assert.Contains("= @p1", plan.CommandText, StringComparison.Ordinal);
    }

    [Fact]
    public void JsonArrayFilters_EmitLengthAndContainmentPredicates()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());

        var plan = planner.Plan(new FindManyQueryModel("Thing", new
        {
            Where = new
            {
                Data = new
                {
                    path = new
                    {
                        path = new[] { "tags" },
                        array_contains = new
                        {
                            HasEvery = new[] { "\"alpha\"", "\"beta\"" },
                            Length = 2,
                            IsEmpty = false
                        }
                    }
                }
            }
        }));

        Assert.Contains("jsonb_typeof", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("jsonb_array_length", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("@>", plan.CommandText, StringComparison.Ordinal);
    }

    [Fact]
    public void JsonPath_SafelyEscapesSingleQuotesInsideSegments()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());

        var plan = planner.Plan(new FindManyQueryModel("Thing", new
        {
            Where = new
            {
                Data = new
                {
                    path = new
                    {
                        path = new[] { "meta", "na'me" },
                        stringFilter = new { Contains = "x" }
                    }
                }
            }
        }));

        Assert.Contains("na''me", plan.CommandText, StringComparison.Ordinal);
        Assert.DoesNotContain("na'me", plan.CommandText, StringComparison.Ordinal);
    }
}
