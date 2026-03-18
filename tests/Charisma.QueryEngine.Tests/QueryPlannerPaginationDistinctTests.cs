using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Tests;

public class QueryPlannerPaginationDistinctTests
{
    [Fact]
    public void NegativeTake_InvertsOrderDirection()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());

        var plan = planner.Plan(new FindManyQueryModel("Thing", new
        {
            OrderBy = new[] { new { Name = "Asc" } },
            Take = -5
        }));

        Assert.Contains("ORDER BY", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"t0\".\"name\" DESC", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIMIT 5", plan.CommandText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Distinct_UnknownField_ThrowsNotSupportedException()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());

        Assert.Throws<NotSupportedException>(() => planner.Plan(new FindManyQueryModel("Thing", new
        {
            Distinct = new[] { "Name", "NotAField" }
        })));
    }

    [Fact]
    public void CursorPagination_UsesParameterizedComparison()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildThingModel());
        var id = Guid.NewGuid();

        var plan = planner.Plan(new FindManyQueryModel("Thing", new
        {
            Cursor = new { Id = id },
            OrderBy = new[] { new { Id = "Asc" } },
            Take = 10
        }));

        Assert.Contains("\"t0\".\"id\" > @p1", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(plan.Parameters, p => Equals(p.Value, id));
    }

    [Fact]
    public void CursorPagination_WithCompositePrimaryKey_UsesLexicographicPredicate()
    {
        var planner = QueryEngineTestModels.BuildPlanner(QueryEngineTestModels.BuildMembershipModel());
        var userId = Guid.NewGuid();
        var teamId = Guid.NewGuid();

        var plan = planner.Plan(new FindManyQueryModel("Membership", new
        {
            Cursor = new
            {
                ByUserIdAndTeamId = new
                {
                    UserId = userId,
                    TeamId = teamId
                }
            },
            OrderBy = new object[]
            {
                new { UserId = "Asc" },
                new { TeamId = "Asc" }
            },
            Take = 10
        }));

        Assert.Contains("\"t0\".\"userid\" >", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"t0\".\"userid\" =", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"t0\".\"teamid\" >", plan.CommandText, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(plan.Parameters, p => Equals(p.Value, userId));
        Assert.Contains(plan.Parameters, p => Equals(p.Value, teamId));
    }
}
