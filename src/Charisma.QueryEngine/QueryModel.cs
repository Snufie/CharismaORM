namespace Charisma.QueryEngine.Model;

/// <summary>
/// Base IR node for all query operations.
/// </summary>
public abstract record QueryModel(QueryType Type, string ModelName)
{
    /// <summary>
    /// Untyped arguments object produced by generated delegates for this query.
    /// </summary>
    public abstract object Args { get; }
}

public sealed record FindUniqueQueryModel(string ModelName, object Args) : QueryModel(QueryType.FindUnique, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record FindFirstQueryModel(string ModelName, object Args, bool ThrowIfNotFound = false) : QueryModel(QueryType.FindFirst, ModelName)
{
    public override object Args { get; } = Args;
    /// <summary>
    /// When true, query executor should raise if no row matches.
    /// </summary>
    public bool ThrowIfNotFound { get; } = ThrowIfNotFound;
}

public sealed record FindManyQueryModel(string ModelName, object Args) : QueryModel(QueryType.FindMany, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record CreateQueryModel(string ModelName, object Args) : QueryModel(QueryType.Create, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record CreateManyQueryModel(string ModelName, object Args, bool ReturnRecords = false) : QueryModel(QueryType.CreateMany, ModelName)
{
    public override object Args { get; } = Args;
    /// <summary>
    /// When true, executor should return created records instead of only counts.
    /// </summary>
    public bool ReturnRecords { get; } = ReturnRecords;
}

public sealed record UpdateQueryModel(string ModelName, object Args) : QueryModel(QueryType.Update, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record UpdateManyQueryModel(string ModelName, object Args, bool ReturnRecords = false) : QueryModel(QueryType.UpdateMany, ModelName)
{
    public override object Args { get; } = Args;
    /// <summary>
    /// When true, executor should return affected records instead of only counts.
    /// </summary>
    public bool ReturnRecords { get; } = ReturnRecords;
}

public sealed record UpsertQueryModel(string ModelName, object Args) : QueryModel(QueryType.Upsert, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record DeleteQueryModel(string ModelName, object Args) : QueryModel(QueryType.Delete, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record DeleteManyQueryModel(string ModelName, object Args) : QueryModel(QueryType.DeleteMany, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record CountQueryModel(string ModelName, object Args) : QueryModel(QueryType.Count, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record AggregateQueryModel(string ModelName, object Args) : QueryModel(QueryType.Aggregate, ModelName)
{
    public override object Args { get; } = Args;
}

public sealed record GroupByQueryModel(string ModelName, object Args) : QueryModel(QueryType.GroupBy, ModelName)
{
    public override object Args { get; } = Args;
}
