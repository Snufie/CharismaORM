using System.Text;
using Charisma.Schema;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates CharismaClient as the single entry point.
/// </summary>
internal sealed class ClientWriter : IWriter
{
    private readonly string _rootNamespace;

    public ClientWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Builds the generated client entry point and model delegates.
    /// </summary>
    /// <param name="schema">Schema containing all models to expose.</param>
    /// <returns>Compilation unit with a single CharismaClient class.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        var clientClass = BuildClientClass(models);
        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName(_rootNamespace))
            .AddMembers(clientClass);

        var unit = SyntaxFactory.CompilationUnit()
            .AddUsings(
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.IO")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Threading")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Threading.Tasks")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Migration")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Migration.Postgres")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Parser")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.QueryEngine")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.QueryEngine.Execution")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Runtime")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Schema")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Models")))
            .AddMembers(@namespace)
            .NormalizeWhitespace();

        return new[] { unit };
    }

    /// <summary>
    /// Composes the CharismaClient class with runtime wiring, delegates, and transaction helpers.
    /// </summary>
    /// <param name="models">Models used to emit delegate properties.</param>
    private ClassDeclarationSyntax BuildClientClass(IReadOnlyList<ModelDefinition> models)
    {
        var members = new List<MemberDeclarationSyntax>
        {
            BuildRuntimeField(),
            BuildExecutorField(),
            BuildConstructor(),
            BuildInternalConstructor(),
            BuildMigrateFromSchemaPathMethod(),
            BuildDispose()
        };

        members.AddRange(BuildDelegateProperties(models));
        members.Add(BuildTransactionMethod(models));
        members.Add(BuildTransactionContext(models));

        return SyntaxFactory.ClassDeclaration("CharismaClient")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword),
                          SyntaxFactory.Token(SyntaxKind.SealedKeyword),
                          SyntaxFactory.Token(SyntaxKind.PartialKeyword))
            .AddBaseListTypes(
                SyntaxFactory.SimpleBaseType(
                    SyntaxFactory.ParseTypeName("IDisposable")))
            .AddMembers(members.ToArray());
    }

    /// <summary>
    /// Declares the runtime backing field.
    /// </summary>
    private static FieldDeclarationSyntax BuildRuntimeField()
    {
        return SyntaxFactory.FieldDeclaration(
                SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("CharismaRuntime"))
                    .AddVariables(
                        SyntaxFactory.VariableDeclarator("_runtime")))
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PrivateKeyword),
                          SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword));
    }

    /// <summary>
    /// Declares the executor backing field.
    /// </summary>
    private static FieldDeclarationSyntax BuildExecutorField()
    {
        return SyntaxFactory.FieldDeclaration(
                SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("ISqlExecutor"))
                    .AddVariables(
                        SyntaxFactory.VariableDeclarator("_executor")))
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PrivateKeyword),
                          SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword));
    }

    private ConstructorDeclarationSyntax BuildConstructor()
    {
        return SyntaxFactory.ConstructorDeclaration("CharismaClient")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("options"))
                    .WithType(SyntaxFactory.IdentifierName("CharismaRuntimeOptions")))
            .WithBody(
                SyntaxFactory.Block(
                        SyntaxFactory.ParseStatement("if (options is null) throw new ArgumentNullException(nameof(options));"),
                        SyntaxFactory.ParseStatement("var runtimeOptions = new CharismaRuntimeOptions { ConnectionString = options.ConnectionString, Provider = options.Provider, RootNamespace = options.RootNamespace, GeneratedAssembly = options.GeneratedAssembly ?? typeof(CharismaClient).Assembly, MetadataRegistry = options.MetadataRegistry, ModelTypeResolver = options.ModelTypeResolver, PreserveIdentifierCasing = options.PreserveIdentifierCasing, MaxNestingDepth = options.MaxNestingDepth, GlobalOmit = options.GlobalOmit, ConnectionProvider = options.ConnectionProvider };"),
                    SyntaxFactory.ParseStatement("_runtime = new CharismaRuntime(runtimeOptions);"),
                    SyntaxFactory.ParseStatement("_executor = _runtime.SqlExecutor;")
                ))
            .WithLeadingTrivia(BuildDoc(
                "Constructs a Charisma client with the provided runtime options.",
                new[] { ("options", "Connection, provider, and namespace configuration.") }));
    }

    private ConstructorDeclarationSyntax BuildInternalConstructor()
    {
        return SyntaxFactory.ConstructorDeclaration("CharismaClient")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PrivateKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("runtime"))
                    .WithType(SyntaxFactory.IdentifierName("CharismaRuntime")),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("executor"))
                    .WithType(SyntaxFactory.IdentifierName("ISqlExecutor")))
            .WithBody(
                SyntaxFactory.Block(
                    SyntaxFactory.ParseStatement("if (runtime is null) throw new ArgumentNullException(nameof(runtime));"),
                    SyntaxFactory.ParseStatement("if (executor is null) throw new ArgumentNullException(nameof(executor));"),
                    SyntaxFactory.ParseStatement("_runtime = runtime;"),
                    SyntaxFactory.ParseStatement("_executor = executor;")
                ))
            .WithLeadingTrivia(BuildDoc(
                "Creates a scoped client bound to an existing runtime and executor.",
                new[] { ("runtime", "Existing runtime instance."), ("executor", "Scoped query executor.") }));
    }

    /// <summary>
    /// Builds a startup migration helper that accepts a schema file path for one-liner startup usage.
    /// </summary>
    private static MethodDeclarationSyntax BuildMigrateFromSchemaPathMethod()
    {
        return SyntaxFactory.MethodDeclaration(
                SyntaxFactory.ParseTypeName("Task<MigrationPlan>"),
                "MigrateAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("schemaPath"))
                    .WithType(SyntaxFactory.ParseTypeName("string"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal("schema.charisma")))),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("options"))
                    .WithType(SyntaxFactory.ParseTypeName("PostgresMigrationOptions?"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression))),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
                    .WithType(SyntaxFactory.ParseTypeName("CancellationToken"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.IdentifierName("default"))))
            .WithBody(SyntaxFactory.Block(
                SyntaxFactory.ParseStatement("if (string.IsNullOrWhiteSpace(schemaPath)) throw new ArgumentException(\"Schema path is required.\", nameof(schemaPath));"),
                SyntaxFactory.ParseStatement("if (!File.Exists(schemaPath)) throw new FileNotFoundException(\"Schema file not found.\", schemaPath);"),
                SyntaxFactory.ParseStatement("var schemaText = await File.ReadAllTextAsync(schemaPath, ct).ConfigureAwait(false);"),
                SyntaxFactory.ParseStatement("var schema = new RoslynSchemaParser().Parse(schemaText);"),
                SyntaxFactory.ParseStatement("return await _runtime.MigrateAsync(schema, options, ct).ConfigureAwait(false);")))
            .WithLeadingTrivia(BuildDoc(
                "One-liner startup migration helper that loads and migrates from a schema file path.",
                new[]
                {
                    ("schemaPath", "Path to the schema file. Defaults to schema.charisma."),
                    ("options", "Optional migration safety options."),
                    ("ct", "Cancellation token for the async operation.")
                },
                "The computed migration plan that was applied (or empty when already in sync)."));
    }

    // Removed CharismaSchema-based MigrateAsync overload to avoid exposing internal IR types to application authors.

    /// <summary>
    /// Emits read-only delegate properties for each model.
    /// </summary>
    private static IEnumerable<MemberDeclarationSyntax> BuildDelegateProperties(IReadOnlyList<ModelDefinition> models)
    {
        foreach (var model in models)
        {
            yield return
                SyntaxFactory.PropertyDeclaration(
                        SyntaxFactory.IdentifierName($"{model.Name}Delegate"),
                        model.Name)
                    .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                    .WithExpressionBody(
                        SyntaxFactory.ArrowExpressionClause(
                            SyntaxFactory.ObjectCreationExpression(
                                    SyntaxFactory.IdentifierName($"{model.Name}Delegate"))
                                .WithArgumentList(
                                    SyntaxFactory.ArgumentList(
                                        SyntaxFactory.SingletonSeparatedList(
                                            SyntaxFactory.Argument(
                                                SyntaxFactory.IdentifierName("_executor")))))))
                    .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken))
                    .WithLeadingTrivia(BuildDoc($"Entry point for {model.Name} queries."));
        }
    }

    /// <summary>
    /// Builds TransactionAsync wrapper that scopes a client within a transaction.
    /// </summary>
    /// <param name="models">Models used for transaction context delegates.</param>
    private MethodDeclarationSyntax BuildTransactionMethod(IReadOnlyList<ModelDefinition> models)
    {
        var statements = SyntaxFactory.List(new StatementSyntax[]
        {
            SyntaxFactory.ParseStatement("if (action is null) throw new ArgumentNullException(nameof(action));"),
            SyntaxFactory.ParseStatement(
                "return _runtime.SqlExecutor.TransactionAsync(async scope => { var scopedClient = new CharismaClient(_runtime, scope.Executor); var tx = new TransactionContext(scope, scopedClient); await action(tx).ConfigureAwait(false); }, ct);")
        });

        return SyntaxFactory.MethodDeclaration(
                SyntaxFactory.IdentifierName("Task"),
                "TransactionAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("action"))
                    .WithType(
                        SyntaxFactory.GenericName("Func")
                            .AddTypeArgumentListArguments(
                                SyntaxFactory.IdentifierName("TransactionContext"),
                                SyntaxFactory.IdentifierName("Task"))),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
                    .WithType(SyntaxFactory.IdentifierName("CancellationToken"))
                    .WithDefault(
                        SyntaxFactory.EqualsValueClause(
                            SyntaxFactory.DefaultExpression(SyntaxFactory.IdentifierName("CancellationToken")))))
            .WithBody(SyntaxFactory.Block(statements))
            .WithLeadingTrivia(BuildDoc(
                "Runs a set of queries inside a transaction.",
                new[]
                {
                    ("action", "Work to execute within the transaction context."),
                    ("ct", "Cancellation token for the async operation.")
                },
                "A task that completes when the transaction scope finishes."));
    }

    /// <summary>
    /// Builds the TransactionContext nested class for transactional delegate access.
    /// </summary>
    /// <param name="models">Models exposed on the context.</param>
    private ClassDeclarationSyntax BuildTransactionContext(IReadOnlyList<ModelDefinition> models)
    {
        var classText = new StringBuilder();
        classText.AppendLine("/// <summary>Transaction-scoped API surface for generated delegates.</summary>");
        classText.AppendLine("public sealed class TransactionContext");
        classText.AppendLine("{");
        classText.AppendLine("    private readonly ITransactionScope _scope;");
        classText.AppendLine("    private readonly CharismaClient _client;");
        classText.AppendLine();
        classText.AppendLine("    /// <summary>Creates a transaction context wrapper.</summary>");
        classText.AppendLine("    /// <param name=\"scope\">Underlying transaction scope.</param>");
        classText.AppendLine("    /// <param name=\"client\">Scoped client bound to the transaction executor.</param>");
        classText.AppendLine("    public TransactionContext(ITransactionScope scope, CharismaClient client)");
        classText.AppendLine("    {");
        classText.AppendLine("        _scope = scope ?? throw new ArgumentNullException(nameof(scope));");
        classText.AppendLine("        _client = client ?? throw new ArgumentNullException(nameof(client));");
        classText.AppendLine("    }");
        classText.AppendLine();
        foreach (var model in models)
        {
            classText.AppendLine($"    /// <summary>Delegate access for {model.Name} operations within the transaction.</summary>");
            classText.AppendLine($"    public {model.Name}Delegate {model.Name} => _client.{model.Name};");
        }
        classText.AppendLine();
        classText.AppendLine("    /// <summary>Marks the transaction for rollback with an optional reason.</summary>");
        classText.AppendLine("    /// <param name=\"reason\">Optional description of the failure cause.</param>");
        classText.AppendLine("    public void FailAndRollback(string? reason = null) => _scope.FailAndRollback(reason);");
        classText.AppendLine("}");

        return (ClassDeclarationSyntax)SyntaxFactory.ParseMemberDeclaration(classText.ToString())!;
    }

    /// <summary>
    /// Disposes the underlying runtime.
    /// </summary>
    private MethodDeclarationSyntax BuildDispose()
    {
        return SyntaxFactory.MethodDeclaration(
                SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.VoidKeyword)),
                "Dispose")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .WithBody(
                SyntaxFactory.Block(
                    SyntaxFactory.ExpressionStatement(
                        SyntaxFactory.InvocationExpression(
                                SyntaxFactory.MemberAccessExpression(
                                    SyntaxKind.SimpleMemberAccessExpression,
                                    SyntaxFactory.IdentifierName("_runtime"),
                                    SyntaxFactory.IdentifierName("Dispose"))))));
    }

    /// <summary>
    /// Helper to build XML doc trivia for summary/params/returns.
    /// </summary>
    private static SyntaxTriviaList BuildDoc(string summary, IEnumerable<(string Name, string Description)>? parameters = null, string? returns = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"/// <summary>{summary}</summary>");

        if (parameters is not null)
        {
            foreach (var (name, desc) in parameters)
            {
                sb.AppendLine($"/// <param name=\"{name}\">{desc}</param>");
            }
        }

        if (!string.IsNullOrWhiteSpace(returns))
        {
            sb.AppendLine($"/// <returns>{returns}</returns>");
        }

        return SyntaxFactory.ParseLeadingTrivia(sb.ToString());
    }
}