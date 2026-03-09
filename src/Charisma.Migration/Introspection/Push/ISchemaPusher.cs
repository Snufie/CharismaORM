using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;

namespace Charisma.Migration.Introspection.Push;

/// <summary>
/// Applies a Charisma schema to a target database.
/// </summary>
public interface ISchemaPusher
{
    Task PushAsync(CharismaSchema schema, CancellationToken cancellationToken = default);
}
