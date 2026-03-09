using System;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Schema;

namespace Charisma.Migration.Introspection.Push;

/// <summary>
/// Coordinates pushing a schema to a database.
/// </summary>
public sealed class SchemaPushService
{
    private readonly ISchemaPusher _pusher;

    public SchemaPushService(ISchemaPusher pusher)
    {
        _pusher = pusher ?? throw new ArgumentNullException(nameof(pusher));
    }

    public Task PushAsync(CharismaSchema schema, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return _pusher.PushAsync(schema, cancellationToken);
    }
}
