using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Tests;

public class QueryPlannerRelationFilterTests
{
    [Fact]
    public void RelationSome_UsesExistsSubquery()
    {
        var (parent, child) = QueryEngineTestModels.BuildParentChildModels();
        var planner = QueryEngineTestModels.BuildPlanner(parent, child);

        var plan = planner.Plan(new FindManyQueryModel("Parent", new
        {
            Where = new
            {
                Children = new
                {
                    Some = new { Value = new { Contains = "ok" } }
                }
            }
        }));

        Assert.Contains("EXISTS (SELECT 1 FROM", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"rf1\"", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RelationNone_UsesNotExistsSubquery()
    {
        var (parent, child) = QueryEngineTestModels.BuildParentChildModels();
        var planner = QueryEngineTestModels.BuildPlanner(parent, child);

        var plan = planner.Plan(new FindManyQueryModel("Parent", new
        {
            Where = new
            {
                Children = new
                {
                    None = new { Value = new { Equals = "bad" } }
                }
            }
        }));

        Assert.Contains("NOT EXISTS", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RelationEvery_UsesNegatedPredicatePattern()
    {
        var (parent, child) = QueryEngineTestModels.BuildParentChildModels();
        var planner = QueryEngineTestModels.BuildPlanner(parent, child);

        var plan = planner.Plan(new FindManyQueryModel("Parent", new
        {
            Where = new
            {
                Children = new
                {
                    Every = new { Value = new { StartsWith = "safe" } }
                }
            }
        }));

        Assert.Contains("NOT EXISTS", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NOT (", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }
}
