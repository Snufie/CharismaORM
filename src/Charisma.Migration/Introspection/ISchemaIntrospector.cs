using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;

namespace Charisma.Migration;

/// <summary>
/// Introspects a live database into a <see cref="CharismaSchema"/> representation.
/// </summary>
public interface ISchemaIntrospector
{
    /// <summary>
    /// Produce a schema snapshot from the current database state.
    /// </summary>
    Task<CharismaSchema> IntrospectAsync(CancellationToken cancellationToken = default);
}
