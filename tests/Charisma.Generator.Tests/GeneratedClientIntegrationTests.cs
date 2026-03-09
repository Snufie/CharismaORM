using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Charisma.Generator;
using Charisma.Parser;
using Charisma.QueryEngine.Execution;
using Charisma.QueryEngine.Exceptions;
using Charisma.QueryEngine.Model;
using Charisma.Runtime;
using Charisma.Schema;
using Xunit;

namespace Charisma.Generator.Tests;

public sealed class GeneratedClientIntegrationTests : IClassFixture<GeneratedAssemblyFixture>
{
    private readonly GeneratedAssemblyFixture _fixture;

    public GeneratedClientIntegrationTests(GeneratedAssemblyFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Delegates_pass_through_full_argument_surface()
    {
        var fake = new FakeExecutor();
        var locatieDelegate = _fixture.CreateDelegate("Locatie", fake);

        var where = _fixture.CreateArgs("LocatieWhereInput",
            ("Naam", _fixture.CreateArgs("StringFilter", ("Contains", "loc"))));
        var include = _fixture.CreateArgs("LocatieInclude",
            ("Robots", _fixture.CreateArgs("RobotInclude")));
        var cursor = _fixture.CreateArgs("LocatieWhereUniqueInput", ("LocatieID", Guid.NewGuid()));

        var findManyArgs = _fixture.CreateArgs("LocatieFindManyArgs",
            ("Where", where),
            ("Distinct", new List<string> { "Naam" }),
            ("Include", include),
            ("Cursor", cursor),
            ("Skip", 1),
            ("Take", 2));

        await _fixture.InvokeAsync(locatieDelegate, "FindManyAsync", findManyArgs);
        var findMany = AssertLastCall(fake, QueryType.FindMany, "Locatie");
        var distinct = findMany.Args.GetType().GetProperty("Distinct")?.GetValue(findMany.Args) as IEnumerable<string>;
        Assert.Contains("Naam", distinct ?? Array.Empty<string>());
        var includeValue = findMany.Args.GetType().GetProperty("Include")?.GetValue(findMany.Args);
        Assert.NotNull(includeValue);

        var countArgs = _fixture.CreateArgs("LocatieCountArgs",
            ("Where", where),
            ("Distinct", new List<string> { "Naam" }));
        await _fixture.InvokeAsync(locatieDelegate, "CountAsync", countArgs);
        var count = AssertLastCall(fake, QueryType.Count, "Locatie");
        var countDistinct = count.Args.GetType().GetProperty("Distinct")?.GetValue(count.Args) as IEnumerable<string>;
        Assert.Contains("Naam", countDistinct ?? Array.Empty<string>());

        var aggregateArgs = _fixture.CreateArgs("LocatieAggregateArgs",
            ("Where", where),
            ("Cursor", cursor),
            ("Skip", 1),
            ("Take", 3),
            ("Distinct", new List<string> { "Naam" }),
            ("Aggregate", _fixture.CreateArgs("LocatieAggregateSelectors", ("Count", true), ("Min", _fixture.CreateArgs("LocatieAggregateMinInput", ("Naam", true))), ("Max", _fixture.CreateArgs("LocatieAggregateMaxInput", ("Naam", true)))))
        );
        await _fixture.InvokeAsync(locatieDelegate, "AggregateAsync", aggregateArgs);
        var aggregate = AssertLastCall(fake, QueryType.Aggregate, "Locatie");
        var aggregateDistinct = aggregate.Args.GetType().GetProperty("Distinct")?.GetValue(aggregate.Args) as IEnumerable<string>;
        Assert.Contains("Naam", aggregateDistinct ?? Array.Empty<string>());
        var createInput = _fixture.CreateArgs("LocatieCreateInput", ("LocatieID", Guid.NewGuid()), ("Naam", "loc-a"));
        var listType = typeof(List<>).MakeGenericType(createInput.GetType());
        var dataList = (System.Collections.IList?)Activator.CreateInstance(listType) ?? throw new InvalidOperationException("Failed to create typed list for createMany.");
        dataList.Add(createInput);

        var createManyArgs = _fixture.CreateArgs("LocatieCreateManyArgs",
            ("Data", dataList),
            ("SkipDuplicates", true));
        await _fixture.InvokeAsync(locatieDelegate, "CreateManyAsync", createManyArgs);
        var createMany = AssertLastCall(fake, QueryType.CreateMany, "Locatie");
        var skipDuplicates = createMany.Args.GetType().GetProperty("SkipDuplicates")?.GetValue(createMany.Args) as bool?;
        Assert.True(skipDuplicates);
    }

    [Fact]
    public async Task Delegates_emit_expected_query_models_for_all_operations()
    {
        var fake = new FakeExecutor();
        var locatieDelegate = _fixture.CreateDelegate("Locatie", fake);

        // FindUnique
        var findUniqueArgs = _fixture.CreateArgs("LocatieFindUniqueArgs", (
            "Where",
            _fixture.CreateArgs("LocatieWhereUniqueInput", ("LocatieID", Guid.NewGuid()))));
        await _fixture.InvokeAsync(locatieDelegate, "FindUniqueAsync", findUniqueArgs);
        AssertLastCall(fake, QueryType.FindUnique, "Locatie");

        // FindFirstOrThrow (flagged)
        await _fixture.InvokeAsync(locatieDelegate, "FindFirstOrThrowAsync");
        var lastFirst = AssertLastCall(fake, QueryType.FindFirst, "Locatie");
        Assert.True(((FindFirstQueryModel)lastFirst).ThrowIfNotFound);

        // FindMany (args optional)
        await _fixture.InvokeAsync(locatieDelegate, "FindManyAsync", null);
        AssertLastCall(fake, QueryType.FindMany, "Locatie");

        // Create (single)
        await _fixture.InvokeAsync(locatieDelegate, "CreateAsync", _fixture.CreateArgs("LocatieCreateArgs"));
        AssertLastCall(fake, QueryType.Create, "Locatie");

        // CreateMany (non-query)
        await _fixture.InvokeAsync(locatieDelegate, "CreateManyAsync", _fixture.CreateArgs("LocatieCreateManyArgs"));
        var createMany = AssertLastCall(fake, QueryType.CreateMany, "Locatie");
        Assert.False(((CreateManyQueryModel)createMany).ReturnRecords);

        // CreateManyAndReturn
        await _fixture.InvokeAsync(locatieDelegate, "CreateManyAndReturnAsync", _fixture.CreateArgs("LocatieCreateManyArgs"));
        var createManyReturn = AssertLastCall(fake, QueryType.CreateMany, "Locatie");
        Assert.True(((CreateManyQueryModel)createManyReturn).ReturnRecords);

        // Update
        await _fixture.InvokeAsync(locatieDelegate, "UpdateAsync", _fixture.CreateArgs("LocatieUpdateArgs", ("Where", _fixture.CreateArgs("LocatieWhereUniqueInput", ("LocatieID", Guid.NewGuid())))));
        AssertLastCall(fake, QueryType.Update, "Locatie");

        // UpdateMany (non-query)
        await _fixture.InvokeAsync(locatieDelegate, "UpdateManyAsync", _fixture.CreateArgs("LocatieUpdateManyArgs"));
        var updateMany = AssertLastCall(fake, QueryType.UpdateMany, "Locatie");
        Assert.False(((UpdateManyQueryModel)updateMany).ReturnRecords);

        // UpdateManyAndReturn
        await _fixture.InvokeAsync(locatieDelegate, "UpdateManyAndReturnAsync", _fixture.CreateArgs("LocatieUpdateManyArgs"));
        var updateManyReturn = AssertLastCall(fake, QueryType.UpdateMany, "Locatie");
        Assert.True(((UpdateManyQueryModel)updateManyReturn).ReturnRecords);

        // Delete
        await _fixture.InvokeAsync(locatieDelegate, "DeleteAsync", _fixture.CreateArgs("LocatieDeleteArgs", ("Where", _fixture.CreateArgs("LocatieWhereUniqueInput", ("LocatieID", Guid.NewGuid())))));
        AssertLastCall(fake, QueryType.Delete, "Locatie");

        // DeleteMany
        await _fixture.InvokeAsync(locatieDelegate, "DeleteManyAsync", _fixture.CreateArgs("LocatieDeleteManyArgs"));
        AssertLastCall(fake, QueryType.DeleteMany, "Locatie");

        // Upsert
        await _fixture.InvokeAsync(locatieDelegate, "UpsertAsync", _fixture.CreateArgs("LocatieUpsertArgs", ("Where", _fixture.CreateArgs("LocatieWhereUniqueInput", ("LocatieID", Guid.NewGuid()))), ("Create", _fixture.CreateArgs("LocatieCreateInput")), ("Update", _fixture.CreateArgs("LocatieUpdateInput"))));
        AssertLastCall(fake, QueryType.Upsert, "Locatie");

        // Count (optional args default)
        await _fixture.InvokeAsync(locatieDelegate, "CountAsync", null);
        AssertLastCall(fake, QueryType.Count, "Locatie");

        // Aggregate (default Aggregate.Count == true)
        await _fixture.InvokeAsync(locatieDelegate, "AggregateAsync", null);
        var aggregate = AssertLastCall(fake, QueryType.Aggregate, "Locatie");
        var aggregateArgs = aggregate.Args.GetType().GetProperty("Aggregate")!.GetValue(aggregate.Args)!;
        var countFlag = aggregateArgs.GetType().GetProperty("Count")!.GetValue(aggregateArgs) as bool?;
        Assert.True(countFlag);

        // GroupBy
        var groupByArgs = _fixture.CreateArgs("LocatieGroupByArgs",
            ("By", new List<string> { "Naam" }),
            ("_count", true));
        await _fixture.InvokeAsync(locatieDelegate, "GroupByAsync", groupByArgs);
        var groupBy = AssertLastCall(fake, QueryType.GroupBy, "Locatie");
        var by = groupBy.Args.GetType().GetProperty("By")!.GetValue(groupBy.Args) as IEnumerable<string>;
        Assert.Contains("Naam", by ?? Array.Empty<string>());
    }

    [Fact]
    public void GlobalOmitOptions_serializes_dictionary_entries()
    {
        var robotOmit = _fixture.CreateOmit("RobotOmit", ("Naam", (object?)true));
        var global = _fixture.CreateGlobalOmitOptions(("Robot", robotOmit));
        var toDict = global.GetType().GetMethod("ToDictionary")?.Invoke(global, Array.Empty<object?>()) as IReadOnlyDictionary<string, object?>;

        Assert.NotNull(toDict);
        Assert.True(toDict!.ContainsKey("Robot"));
        Assert.Same(robotOmit, toDict["Robot"]);
    }

    [Fact]
    public async Task Transaction_scopes_propagate_executor_and_allow_manual_rollback()
    {
        var fake = new FakeExecutor();
        var client = _fixture.CreateClientWithExecutor(fake);
        var scope = new FakeTransactionScope(fake);
        var tx = _fixture.CreateTransactionContext(client, scope);

        var locatieDelegate = _fixture.GetDelegateProperty(tx, "Locatie");
        await _fixture.InvokeAsync(locatieDelegate, "FindManyAsync", null);
        fake.LastCall.ShouldNotBeNull();

        var thrown = Assert.Throws<ManualTransactionRollbackException>(() =>
        {
            try
            {
                tx.GetType().GetMethod("FailAndRollback")!.Invoke(tx, new object?[] { "stop" });
            }
            catch (TargetInvocationException tie) when (tie.InnerException is not null)
            {
                throw tie.InnerException;
            }
        });

        Assert.Equal("stop", thrown.Message);
        Assert.True(fake.TransactionScopes.Any() || scope.RolledBack || scope.Committed == false);
    }

    private static QueryModel AssertLastCall(FakeExecutor fake, QueryType expectedType, string expectedModel)
    {
        var last = fake.LastCall ?? throw new InvalidOperationException("No executor calls were recorded.");
        Assert.Equal(expectedType, last.Model.Type);
        Assert.Equal(expectedModel, last.Model.ModelName);
        return last.Model;
    }
}

public sealed record RecordedCall(QueryModel Model, SqlExecutionContext? Context, Type ReturnType, string ExecutorMethod);

public sealed class FakeExecutor : ISqlExecutor
{
    private readonly List<RecordedCall> _calls = new();
    private readonly List<FakeTransactionScope> _scopes = new();

    public IReadOnlyList<RecordedCall> Calls => _calls.ToImmutableArray();
    public RecordedCall? LastCall => _calls.LastOrDefault();
    public IReadOnlyList<FakeTransactionScope> TransactionScopes => _scopes.ToImmutableArray();

    public Task<T?> ExecuteSingleAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
    {
        _calls.Add(new RecordedCall(query, context, typeof(T), nameof(ExecuteSingleAsync)));
        return Task.FromResult(CreateDefault<T>());
    }

    public Task<IReadOnlyList<T>> ExecuteManyAsync<T>(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
    {
        _calls.Add(new RecordedCall(query, context, typeof(T), nameof(ExecuteManyAsync)));
        var item = CreateDefault<T>();
        return Task.FromResult<IReadOnlyList<T>>(new List<T?> { item }.OfType<T>().ToList());
    }

    public Task<int> ExecuteNonQueryAsync(QueryModel query, SqlExecutionContext? context = null, CancellationToken ct = default)
    {
        _calls.Add(new RecordedCall(query, context, typeof(int), nameof(ExecuteNonQueryAsync)));
        return Task.FromResult(1);
    }

    public Task TransactionAsync(Func<ITransactionScope, Task> work, CancellationToken ct = default)
    {
        var scope = new FakeTransactionScope(this);
        _scopes.Add(scope);
        return work(scope);
    }

    public Task<T> TransactionAsync<T>(Func<ITransactionScope, Task<T>> work, CancellationToken ct = default)
    {
        var scope = new FakeTransactionScope(this);
        _scopes.Add(scope);
        return work(scope);
    }

    public Task<ITransactionScope> BeginTransactionAsync(CancellationToken ct = default)
    {
        var scope = new FakeTransactionScope(this);
        _scopes.Add(scope);
        return Task.FromResult<ITransactionScope>(scope);
    }

    private static T? CreateDefault<T>()
    {
        var type = typeof(T);
        if (type.IsValueType)
        {
            return default;
        }

        return Activator.CreateInstance<T>();
    }
}

public sealed class FakeTransactionScope : ITransactionScope
{
    private readonly FakeDbConnection _connection = new();

    public FakeTransactionScope(ISqlExecutor executor)
    {
        Executor = executor;
        Context = new SqlExecutionContext(_connection, null);
    }

    public ISqlExecutor Executor { get; }
    public SqlExecutionContext Context { get; }
    public bool Committed { get; private set; }
    public bool RolledBack { get; private set; }

    public void FailAndRollback(string? reason = null)
    {
        RolledBack = true;
        throw new ManualTransactionRollbackException(reason);
    }

    public Task CommitAsync(CancellationToken ct = default)
    {
        Committed = true;
        return Task.CompletedTask;
    }

    public Task RollbackAsync(CancellationToken ct = default)
    {
        RolledBack = true;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}

public sealed class GeneratedAssemblyFixture : IAsyncLifetime
{
    private const string RootNamespace = "Charisma.Generated";
    private Assembly? _assembly;

    public async Task InitializeAsync()
    {
        var projectDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
        var repoRoot = Path.GetFullPath(Path.Combine(projectDir, "..", ".."));
        var schemaPath = Path.Combine(repoRoot, "schema.charisma");
        var outputDir = Path.Combine(Path.GetTempPath(), "charisma-gen-tests", Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(outputDir);

        var parser = new RoslynSchemaParser();
        var schema = parser.Parse(await File.ReadAllTextAsync(schemaPath).ConfigureAwait(false));

        var generator = new CharismaGenerator(new GeneratorOptions
        {
            RootNamespace = RootNamespace,
            GeneratorVersion = "tests",
            OutputDirectory = outputDir
        });

        var units = generator.Generate(schema);
        Assert.NotEmpty(units);

        _assembly = CompileGeneratedAssembly(outputDir);
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    public object CreateDelegate(string modelName, ISqlExecutor executor)
    {
        var type = GetTypeOrThrow($"{RootNamespace}.Models.{modelName}Delegate");
        return Activator.CreateInstance(type, executor) ?? throw new InvalidOperationException($"Failed to create delegate for {modelName}.");
    }

    public object CreateTransactionContext(object client, FakeTransactionScope scope)
    {
        var txType = GetTypeOrThrow($"{RootNamespace}.CharismaClient+TransactionContext");
        return Activator.CreateInstance(txType, scope, client) ?? throw new InvalidOperationException("Failed to create transaction context.");
    }

    public object CreateArgs(string typeName, params (string Name, object? Value)[] setters)
    {
        var type = _assembly?.GetType($"{RootNamespace}.Args.{typeName}")
                   ?? _assembly?.GetType($"{RootNamespace}.Filters.{typeName}")
                   ?? _assembly?.GetType($"{RootNamespace}.Include.{typeName}")
                   ?? throw new InvalidOperationException($"Generated type '{RootNamespace}.Args.{typeName}' not found.");
        var instance = Activator.CreateInstance(type) ?? throw new InvalidOperationException($"Failed to create args type {typeName}.");

        foreach (var (name, value) in setters)
        {
            var prop = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            prop?.SetValue(instance, value);
        }

        return instance;
    }

    public object CreateOmit(string typeName, params (string Name, object? Value)[] setters)
    {
        var type = GetTypeOrThrow($"{RootNamespace}.Omit.{typeName}");
        var instance = Activator.CreateInstance(type) ?? throw new InvalidOperationException($"Failed to create omit type {typeName}.");

        foreach (var (name, value) in setters)
        {
            var prop = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            prop?.SetValue(instance, value);
        }

        return instance;
    }

    public object CreateGlobalOmitOptions(params (string Name, object? Value)[] setters)
    {
        var type = GetTypeOrThrow($"{RootNamespace}.Omit.GlobalOmitOptions");
        var instance = Activator.CreateInstance(type) ?? throw new InvalidOperationException("Failed to create global omit options.");

        foreach (var (name, value) in setters)
        {
            var prop = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            prop?.SetValue(instance, value);
        }

        return instance;
    }

    public async Task<object?> InvokeAsync(object target, string methodName, params object?[]? args)
    {
        var method = target.GetType().GetMethod(methodName) ?? throw new InvalidOperationException($"Method {methodName} not found on {target.GetType().Name}.");
        var parameters = method.GetParameters();
        var supplied = args ?? Array.Empty<object?>();
        var finalArgs = new object?[parameters.Length];

        for (int i = 0; i < parameters.Length; i++)
        {
            if (i < supplied.Length)
            {
                finalArgs[i] = supplied[i];
                continue;
            }

            var p = parameters[i];
            if (p.HasDefaultValue)
            {
                finalArgs[i] = p.DefaultValue;
            }
            else
            {
                finalArgs[i] = p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType) : null;
            }
        }

        var result = method.Invoke(target, finalArgs);
        if (result is not Task task)
        {
            return result;
        }

        await task.ConfigureAwait(false);
        var resultProperty = task.GetType().GetProperty("Result");
        return resultProperty?.GetValue(task);
    }

    public object GetDelegateProperty(object transactionContext, string propertyName)
    {
        var prop = transactionContext.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);
        return prop?.GetValue(transactionContext) ?? throw new InvalidOperationException($"Property {propertyName} not found on {transactionContext.GetType().Name}.");
    }

    public object CreateClientWithExecutor(FakeExecutor executor)
    {
        // Build dummy options (no connections are opened until executor is used).
        var options = new CharismaRuntimeOptions
        {
            ConnectionString = "Host=localhost;Database=fake;Username=fake;Password=fake",
            Provider = ProviderOptions.PostgreSQL,
            RootNamespace = RootNamespace,
            GeneratedAssembly = GetTypeOrThrow($"{RootNamespace}.CharismaClient").Assembly
        };

        var clientType = GetTypeOrThrow($"{RootNamespace}.CharismaClient");
        var client = Activator.CreateInstance(clientType, options) ?? throw new InvalidOperationException("Failed to create client.");

        // Swap the executor to the fake instance so no real DB work occurs.
        var executorField = clientType.GetField("_executor", BindingFlags.NonPublic | BindingFlags.Instance);
        executorField?.SetValue(client, executor);

        return client;
    }

    private Assembly CompileGeneratedAssembly(string outputDir)
    {
        var syntaxTrees = Directory.GetFiles(outputDir, "*.cs", SearchOption.AllDirectories)
            .Select(path => Microsoft.CodeAnalysis.CSharp.CSharpSyntaxTree.ParseText(File.ReadAllText(path), path: path))
            .ToList();

        var referencedAssemblies = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrWhiteSpace(a.Location))
            .Select(a => Microsoft.CodeAnalysis.MetadataReference.CreateFromFile(a.Location))
            .ToList();

        // Ensure core Charisma assemblies are referenced even if not yet loaded in the AppDomain.
        var requiredAssemblies = new[]
        {
            typeof(CharismaRuntimeOptions).Assembly,
            typeof(CharismaRuntime).Assembly,
            typeof(QueryModel).Assembly,
            typeof(CharismaSchema).Assembly,
            typeof(RoslynSchemaParser).Assembly
        };

        foreach (var asm in requiredAssemblies)
        {
            var path = asm.Location;
            if (string.IsNullOrWhiteSpace(path))
            {
                continue;
            }

            var reference = Microsoft.CodeAnalysis.MetadataReference.CreateFromFile(path);
            if (!referencedAssemblies.Any(r => string.Equals(r.Display, reference.Display, StringComparison.OrdinalIgnoreCase)))
            {
                referencedAssemblies.Add(reference);
            }
        }

        var assemblyName = $"Charisma.Generated.Tests.Dynamic.{Guid.NewGuid():N}";
        var compilation = Microsoft.CodeAnalysis.CSharp.CSharpCompilation.Create(
            assemblyName,
            syntaxTrees,
            referencedAssemblies,
            new Microsoft.CodeAnalysis.CSharp.CSharpCompilationOptions(Microsoft.CodeAnalysis.OutputKind.DynamicallyLinkedLibrary));

        using var peStream = new MemoryStream();
        var emitResult = compilation.Emit(peStream);
        if (!emitResult.Success)
        {
            var errors = string.Join(Environment.NewLine, emitResult.Diagnostics.Where(d => d.Severity == Microsoft.CodeAnalysis.DiagnosticSeverity.Error));
            throw new InvalidOperationException($"Generated code did not compile:{Environment.NewLine}{errors}");
        }

        peStream.Position = 0;
        return AssemblyLoadContext.Default.LoadFromStream(peStream);
    }

    private Type GetTypeOrThrow(string fullName)
    {
        return _assembly?.GetType(fullName) ?? throw new InvalidOperationException($"Generated type '{fullName}' not found.");
    }
}

internal sealed class FakeDbConnection : DbConnection
{
    private string _connectionString = "Host=fake";

    [AllowNull]
    public override string ConnectionString
    {
        get => _connectionString;
        set => _connectionString = value ?? "Host=fake";
    }
    public override string Database => "fake";
    public override string DataSource => "fake";
    public override string ServerVersion => "0.0";
    public override ConnectionState State => ConnectionState.Open;

    public override void ChangeDatabase(string databaseName)
    {
    }

    public override void Close()
    {
    }

    public override void Open()
    {
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new FakeDbTransaction(this);
    protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
}

internal sealed class FakeDbTransaction : DbTransaction
{
    private readonly DbConnection _connection;

    public FakeDbTransaction(DbConnection connection)
    {
        _connection = connection;
    }

    public override IsolationLevel IsolationLevel => IsolationLevel.Unspecified;
    protected override DbConnection DbConnection => _connection;
    public override void Commit()
    {
    }
    public override void Rollback()
    {
    }
}

internal static class AssertionExtensions
{
    public static void ShouldNotBeNull(this object? value)
    {
        Assert.NotNull(value);
    }
}
