using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;

namespace Charisma.Migration;

/// <summary>
/// Computes a migration plan between a schema file and a live database.
/// </summary>
public interface IMigrationPlanner
{
    Task<MigrationPlan> PlanAsync(CharismaSchema schema, CancellationToken cancellationToken = default);
}
