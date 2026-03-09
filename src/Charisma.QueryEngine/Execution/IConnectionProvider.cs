using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Abstraction for opening provider-specific database connections.
/// Implementations handle pooling and provider-specific configuration.
/// </summary>
public interface IConnectionProvider
{
    /// <summary>
    /// Opens a new database connection.
    /// Caller owns the returned connection and must dispose it.
    /// </summary>
    /// <param name="ct">Cancellation token for opening the connection.</param>
    /// <returns>Open <see cref="DbConnection"/> instance.</returns>
    Task<DbConnection> OpenAsync(CancellationToken ct);
}
