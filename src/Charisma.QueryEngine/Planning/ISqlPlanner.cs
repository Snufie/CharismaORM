using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Planning;

/// <summary>
/// Produces provider-agnostic, parameterized SQL plans from QueryModel inputs.
/// </summary>
public interface ISqlPlanner
{
    SqlPlan Plan(QueryModel query);
}
