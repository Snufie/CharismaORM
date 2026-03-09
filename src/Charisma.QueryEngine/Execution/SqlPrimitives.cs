using System.Data.Common;
using Charisma.QueryEngine.Model;

namespace Charisma.QueryEngine.Execution;

/// <summary>
/// Represents a query that returns rows.
/// The underlying executor translates the <see cref="QueryModel"/> into SQL.
/// </summary>
public sealed record SqlQuery(QueryModel Model);

/// <summary>
/// Represents a mutation/non-query command.
/// The underlying executor translates the <see cref="QueryModel"/> into SQL.
/// </summary>
public sealed record SqlCommand(QueryModel Model);

/// <summary>
/// Carries an ambient connection/transaction for scoped execution.
/// </summary>
public sealed record SqlExecutionContext(DbConnection Connection, DbTransaction? Transaction);
