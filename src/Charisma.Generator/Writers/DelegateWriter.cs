using System.Text;
using Charisma.Schema;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Charisma.QueryEngine.Model;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates per-model delegates that build QueryModel instances and call the executor.
/// </summary>
internal sealed class DelegateWriter : IWriter
{
    private readonly string _rootNamespace;

    public DelegateWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Writes per-model delegate compilation units for the provided schema.
    /// </summary>
    /// <param name="schema">Schema to generate delegates for.</param>
    /// <returns>Ordered list of compilation units, one per model.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var units = new List<CompilationUnitSyntax>();

        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        foreach (var model in models)
        {
            units.Add(BuildDelegateUnit(schema, model));
        }

        return units.AsReadOnly();
    }

    /// <summary>
    /// Builds the compilation unit for a single model delegate.
    /// </summary>
    /// <param name="schema">Schema context for type mapping.</param>
    /// <param name="model">Model to generate.</param>
    /// <returns>Compilation unit containing the delegate class.</returns>
    private CompilationUnitSyntax BuildDelegateUnit(CharismaSchema schema, ModelDefinition model)
    {
        var members = new List<MemberDeclarationSyntax>
        {
            BuildExecutorField(),
            BuildConstructor(model),
            BuildFindUnique(model),
            BuildFindFirst(model),
            BuildFindFirstOrThrow(model),
            BuildFindMany(model),
            BuildCreate(model),
            BuildCreateMany(model),
            BuildCreateManyAndReturn(model),
            BuildUpdate(model),
            BuildUpdateMany(model),
            BuildUpdateManyAndReturn(model),
            BuildDelete(model),
            BuildDeleteMany(model),
            BuildUpsert(model),
            BuildCount(model),
            BuildAggregate(model),
            BuildGroupBy(model)
        };

        var findById = BuildFindById(schema, model);
        if (findById is not null)
        {
            members.Add(findById);
        }

        foreach (var convenience in BuildAdditionalConvenienceMethods(schema, model))
        {
            members.Add(convenience);
        }

        var @class = SyntaxFactory.ClassDeclaration($"{model.Name}Delegate")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.SealedKeyword), SyntaxFactory.Token(SyntaxKind.PartialKeyword))
            .AddMembers(members.ToArray());

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Models"))
            .AddMembers(@class);

        return SyntaxFactory.CompilationUnit()
            .AddUsings(
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Collections.Generic")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Threading")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Threading.Tasks")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.QueryEngine.Execution")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.QueryEngine.Model")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Args")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Models")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Filters")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Select")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Include")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Runtime")))
            .AddMembers(@namespace)
            .NormalizeWhitespace();
    }

    /// <summary>
    /// Emits the executor backing field used by all delegate methods.
    /// </summary>
    /// <returns>Readonly field declaration.</returns>
    private static FieldDeclarationSyntax BuildExecutorField()
    {
        return SyntaxFactory.FieldDeclaration(
                SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("ISqlExecutor"))
                    .AddVariables(
                        SyntaxFactory.VariableDeclarator("_executor")))
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PrivateKeyword), SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword));
    }

    /// <summary>
    /// Creates a public constructor that accepts the shared executor.
    /// </summary>
    /// <param name="model">Model for documentation context.</param>
    /// <returns>Constructor declaration.</returns>
    private static ConstructorDeclarationSyntax BuildConstructor(ModelDefinition model)
    {
        return SyntaxFactory.ConstructorDeclaration($"{model.Name}Delegate")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("executor"))
                    .WithType(SyntaxFactory.IdentifierName("ISqlExecutor")))
            .WithBody(
                SyntaxFactory.Block(
                    SyntaxFactory.ExpressionStatement(
                        SyntaxFactory.AssignmentExpression(
                            SyntaxKind.SimpleAssignmentExpression,
                            SyntaxFactory.IdentifierName("_executor"),
                            SyntaxFactory.IdentifierName("executor")))))
            .WithLeadingTrivia(BuildDoc(
                $"Creates a {model.Name} delegate backed by the provided query executor.",
                new[] { ("executor", "Shared query executor used to dispatch queries.") }));
    }

    /// <summary>
    /// Builds the FindUnique delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildFindUnique(ModelDefinition model)
    {
        var argsType = SyntaxFactory.ParseTypeName($"{model.Name}FindUniqueArgs");
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"),
            name: "FindUniqueAsync",
            argsType: argsType,
            queryType: QueryType.FindUnique,
            modelName: model.Name);
    }

    /// <summary>
    /// Builds the FindMany delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildFindMany(ModelDefinition model)
    {
        var argsType = SyntaxFactory.ParseTypeName($"{model.Name}FindManyArgs");
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<IReadOnlyList<{model.Name}>>"),
            name: "FindManyAsync",
            argsType: argsType,
            queryType: QueryType.FindMany,
            modelName: model.Name,
            argsOptional: true,
            defaultArgsExpression: SyntaxFactory.ObjectCreationExpression(argsType)
                .WithArgumentList(SyntaxFactory.ArgumentList()),
            isMany: true);
    }

    /// <summary>
    /// Builds the FindFirst delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildFindFirst(ModelDefinition model)
    {
        var argsType = SyntaxFactory.ParseTypeName($"{model.Name}FindFirstArgs");
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"),
            name: "FindFirstAsync",
            argsType: argsType,
            queryType: QueryType.FindFirst,
            modelName: model.Name,
            argsOptional: true,
            defaultArgsExpression: SyntaxFactory.ObjectCreationExpression(argsType)
                .WithArgumentList(SyntaxFactory.ArgumentList()));
    }

    /// <summary>
    /// Builds the FindFirstOrThrow delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildFindFirstOrThrow(ModelDefinition model)
    {
        var argsType = SyntaxFactory.ParseTypeName($"{model.Name}FindFirstArgs");
        // Return non-nullable Task<Model> and enforce throw-if-not-found in the generated body.
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}>"),
            name: "FindFirstOrThrowAsync",
            argsType: argsType,
            queryType: QueryType.FindFirst,
            modelName: model.Name,
            argsOptional: true,
            defaultArgsExpression: SyntaxFactory.ObjectCreationExpression(argsType)
                .WithArgumentList(SyntaxFactory.ArgumentList()),
            throwIfNotFound: true);
    }

    /// <summary>
    /// Builds the Create delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildCreate(ModelDefinition model)
    {
        return BuildDelegateMethod(
                returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"),
            name: "CreateAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}CreateArgs"),
            queryType: QueryType.Create,
            modelName: model.Name);
    }

    /// <summary>
    /// Builds the CreateMany delegate method (non-returning) for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildCreateMany(ModelDefinition model)
    {
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName("Task<int>"),
            name: "CreateManyAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}CreateManyArgs"),
            queryType: QueryType.CreateMany,
            modelName: model.Name,
            nonQuery: true,
            returnRecords: false);
    }

    /// <summary>
    /// Builds the CreateManyAndReturn delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildCreateManyAndReturn(ModelDefinition model)
    {
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<IReadOnlyList<{model.Name}>>"),
            name: "CreateManyAndReturnAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}CreateManyArgs"),
            queryType: QueryType.CreateMany,
            modelName: model.Name,
            isMany: true,
            returnRecords: true);
    }

    /// <summary>
    /// Builds the Update delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildUpdate(ModelDefinition model)
    {
        return BuildDelegateMethod(
                returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"),
            name: "UpdateAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}UpdateArgs"),
            queryType: QueryType.Update,
            modelName: model.Name);
    }

    /// <summary>
    /// Builds the UpdateMany delegate method (non-returning) for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildUpdateMany(ModelDefinition model)
    {
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName("Task<int>"),
            name: "UpdateManyAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}UpdateManyArgs"),
            queryType: QueryType.UpdateMany,
            modelName: model.Name,
            nonQuery: true,
            returnRecords: false);
    }

    /// <summary>
    /// Builds the UpdateManyAndReturn delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildUpdateManyAndReturn(ModelDefinition model)
    {
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<IReadOnlyList<{model.Name}>>"),
            name: "UpdateManyAndReturnAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}UpdateManyArgs"),
            queryType: QueryType.UpdateMany,
            modelName: model.Name,
            isMany: true,
            returnRecords: true);
    }

    /// <summary>
    /// Builds the Delete delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildDelete(ModelDefinition model)
    {
        return BuildDelegateMethod(
                returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"),
            name: "DeleteAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}DeleteArgs"),
            queryType: QueryType.Delete,
            modelName: model.Name);
    }

    /// <summary>
    /// Builds the DeleteMany delegate method (non-returning) for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildDeleteMany(ModelDefinition model)
    {
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName("Task<int>"),
            name: "DeleteManyAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}DeleteManyArgs"),
            queryType: QueryType.DeleteMany,
            modelName: model.Name,
            nonQuery: true);
    }

    /// <summary>
    /// Builds the Upsert delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildUpsert(ModelDefinition model)
    {
        return BuildDelegateMethod(
                returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"),
            name: "UpsertAsync",
            argsType: SyntaxFactory.ParseTypeName($"{model.Name}UpsertArgs"),
            queryType: QueryType.Upsert,
            modelName: model.Name);
    }

    /// <summary>
    /// Builds the Count delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildCount(ModelDefinition model)
    {
        var argsType = SyntaxFactory.ParseTypeName($"{model.Name}CountArgs");
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName("Task<int>"),
            name: "CountAsync",
            argsType: argsType,
            queryType: QueryType.Count,
            modelName: model.Name,
            argsOptional: true,
            defaultArgsExpression: SyntaxFactory.ObjectCreationExpression(argsType)
                .WithArgumentList(SyntaxFactory.ArgumentList()),
            returnRecords: false);
    }

    /// <summary>
    /// Builds the Aggregate delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildAggregate(ModelDefinition model)
    {
        var argsType = SyntaxFactory.ParseTypeName($"{model.Name}AggregateArgs");
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<{model.Name}AggregateResult?>"),
            name: "AggregateAsync",
            argsType: argsType,
            queryType: QueryType.Aggregate,
            modelName: model.Name,
            argsOptional: true,
            defaultArgsExpression: SyntaxFactory.ObjectCreationExpression(argsType)
                .WithInitializer(
                    SyntaxFactory.InitializerExpression(
                        SyntaxKind.ObjectInitializerExpression,
                        SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                            SyntaxFactory.AssignmentExpression(
                                SyntaxKind.SimpleAssignmentExpression,
                                SyntaxFactory.IdentifierName("Aggregate"),
                                SyntaxFactory.ObjectCreationExpression(
                                        SyntaxFactory.ParseTypeName($"{model.Name}AggregateSelectors"))
                                    .WithInitializer(
                                        SyntaxFactory.InitializerExpression(
                                            SyntaxKind.ObjectInitializerExpression,
                                            SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                                                SyntaxFactory.AssignmentExpression(
                                                    SyntaxKind.SimpleAssignmentExpression,
                                                    SyntaxFactory.IdentifierName("Count"),
                                                    SyntaxFactory.LiteralExpression(SyntaxKind.TrueLiteralExpression))))))))),
            returnRecords: true,
            resultTypeOverride: $"{model.Name}AggregateResult");
    }

    /// <summary>
    /// Builds the GroupBy delegate method for the model.
    /// </summary>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration.</returns>
    private MethodDeclarationSyntax BuildGroupBy(ModelDefinition model)
    {
        var argsType = SyntaxFactory.ParseTypeName($"{model.Name}GroupByArgs");
        return BuildDelegateMethod(
            returnType: SyntaxFactory.ParseTypeName($"Task<IReadOnlyList<{model.Name}GroupByOutput>>"),
            name: "GroupByAsync",
            argsType: argsType,
            queryType: QueryType.GroupBy,
            modelName: model.Name,
            isMany: true,
            returnRecords: true,
            resultTypeOverride: $"{model.Name}GroupByOutput");
    }

    /// <summary>
    /// Builds a FindById helper when the primary key is a single scalar field.
    /// </summary>
    /// <param name="schema">Schema context.</param>
    /// <param name="model">Target model.</param>
    /// <returns>Method declaration when applicable; otherwise null.</returns>
    private MethodDeclarationSyntax? BuildFindById(CharismaSchema schema, ModelDefinition model)
    {
        if (model.PrimaryKey is not { } pk || pk.Fields.Count != 1)
        {
            return null;
        }

        var pkFieldName = pk.Fields[0];
        var pkField = model.GetField(pkFieldName) as ScalarFieldDefinition;
        if (pkField is null)
        {
            return null;
        }

        var idType = MapScalarType(schema, pkField.RawType, nullable: false);

        var uniqueArgsType = SyntaxFactory.ParseTypeName($"{model.Name}FindUniqueArgs");
        var whereUniqueType = SyntaxFactory.ParseTypeName($"{model.Name}WhereUniqueInput");
        var bodyText = $@"{{
    var argsValue = args ?? new {uniqueArgsType}
    {{
        Where = new {whereUniqueType} {{ {pkFieldName} = id }}
    }};

    argsValue.Where ??= new {whereUniqueType} {{ {pkFieldName} = id }};
    return await FindUniqueAsync(argsValue, ct).ConfigureAwait(false);
}}";

        var methodBody = SyntaxFactory.ParseStatement(bodyText) as BlockSyntax
                          ?? SyntaxFactory.Block();

        return SyntaxFactory.MethodDeclaration(
                SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"),
                "FindByIdAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("id"))
                    .WithType(SyntaxFactory.ParseTypeName(idType)),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("args"))
                    .WithType(SyntaxFactory.ParseTypeName($"{model.Name}FindUniqueArgs?"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression))),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
                    .WithType(SyntaxFactory.ParseTypeName("CancellationToken"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.IdentifierName("default"))))
            .WithBody(methodBody)
            .WithLeadingTrivia(BuildDoc(
                $"Convenience helper to find {model.Name} by primary key.",
                new[]
                {
                    ("id", $"Primary key value of the {model.Name} to fetch."),
                    ("args", "Optional args controlling select/include."),
                    ("ct", "Cancellation token for the async operation.")
                },
                $"The matching {model.Name} instance or null."));
    }

    /// <summary>
    /// Builds additional high-level convenience methods for generated delegates.
    /// </summary>
    private IEnumerable<MethodDeclarationSyntax> BuildAdditionalConvenienceMethods(CharismaSchema schema, ModelDefinition model)
    {
        yield return BuildExists(model);

        if (model.PrimaryKey is not { } pk || pk.Fields.Count == 0)
        {
            yield break;
        }

        if (pk.Fields.Count == 1)
        {
            var singleField = model.GetField(pk.Fields[0]) as ScalarFieldDefinition;
            if (singleField is not null)
            {
                yield return BuildDeleteBySinglePrimaryKey(schema, model, singleField);
                yield return BuildUpdateBySinglePrimaryKey(schema, model, singleField);
            }
            yield break;
        }

        var compositeFields = pk.Fields
            .Select(name => model.GetField(name) as ScalarFieldDefinition)
            .Where(f => f is not null)
            .Cast<ScalarFieldDefinition>()
            .ToList();

        if (compositeFields.Count != pk.Fields.Count)
        {
            yield break;
        }

        var selector = BuildCompositePrimaryKeySelector(model, pk);
        yield return BuildFindByCompositePrimaryKey(schema, model, selector, compositeFields);
        yield return BuildDeleteByCompositePrimaryKey(schema, model, selector, compositeFields);
        yield return BuildUpdateByCompositePrimaryKey(schema, model, selector, compositeFields);
    }

    private static MethodDeclarationSyntax BuildExists(ModelDefinition model)
    {
        var body = SyntaxFactory.Block(
            SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(SyntaxFactory.ParseTypeName($"{model.Name}CountArgs"))
                    .AddVariables(
                        SyntaxFactory.VariableDeclarator("countArgs")
                            .WithInitializer(
                                SyntaxFactory.EqualsValueClause(
                                    SyntaxFactory.ObjectCreationExpression(SyntaxFactory.ParseTypeName($"{model.Name}CountArgs"))
                                        .WithInitializer(
                                            SyntaxFactory.InitializerExpression(
                                                SyntaxKind.ObjectInitializerExpression,
                                                SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                                                    SyntaxFactory.AssignmentExpression(
                                                        SyntaxKind.SimpleAssignmentExpression,
                                                        SyntaxFactory.IdentifierName("Where"),
                                                        SyntaxFactory.IdentifierName("where"))))))))),
            SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(SyntaxFactory.ParseTypeName("var"))
                    .AddVariables(
                        SyntaxFactory.VariableDeclarator("count")
                            .WithInitializer(
                                SyntaxFactory.EqualsValueClause(
                                    SyntaxFactory.AwaitExpression(
                                        SyntaxFactory.InvocationExpression(SyntaxFactory.IdentifierName("CountAsync"))
                                            .WithArgumentList(
                                                SyntaxFactory.ArgumentList(
                                                    SyntaxFactory.SeparatedList(new[]
                                                    {
                                                        SyntaxFactory.Argument(SyntaxFactory.IdentifierName("countArgs")),
                                                        SyntaxFactory.Argument(SyntaxFactory.IdentifierName("ct"))
                                                    })))))))),
            SyntaxFactory.ReturnStatement(
                SyntaxFactory.BinaryExpression(
                    SyntaxKind.GreaterThanExpression,
                    SyntaxFactory.IdentifierName("count"),
                    SyntaxFactory.LiteralExpression(SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(0)))));

        return SyntaxFactory.MethodDeclaration(SyntaxFactory.ParseTypeName("Task<bool>"), "ExistsAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("where"))
                    .WithType(SyntaxFactory.ParseTypeName($"{model.Name}WhereInput?"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression))),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
                    .WithType(SyntaxFactory.ParseTypeName("CancellationToken"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.IdentifierName("default"))))
            .WithBody(body)
            .WithLeadingTrivia(BuildDoc(
                $"Checks whether at least one {model.Name} exists for the optional filter.",
                new[]
                {
                    ("where", "Optional filter restricting which rows are considered."),
                    ("ct", "Cancellation token for the async operation.")
                },
                "True when a matching row exists; otherwise false."));
    }

    private MethodDeclarationSyntax BuildDeleteBySinglePrimaryKey(CharismaSchema schema, ModelDefinition model, ScalarFieldDefinition pkField)
    {
        var idType = MapScalarType(schema, pkField.RawType, nullable: false);
        var bodyText = $@"{{
    var argsValue = args ?? new {model.Name}DeleteArgs
    {{
        Where = new {model.Name}WhereUniqueInput {{ {pkField.Name} = id }}
    }};

    argsValue.Where ??= new {model.Name}WhereUniqueInput {{ {pkField.Name} = id }};
    return await DeleteAsync(argsValue, ct).ConfigureAwait(false);
}}";

        return SyntaxFactory.MethodDeclaration(SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"), "DeleteByIdAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("id")).WithType(SyntaxFactory.ParseTypeName(idType)),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("args"))
                    .WithType(SyntaxFactory.ParseTypeName($"{model.Name}DeleteArgs?"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression))),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
                    .WithType(SyntaxFactory.ParseTypeName("CancellationToken"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.IdentifierName("default"))))
            .WithBody(SyntaxFactory.ParseStatement(bodyText) as BlockSyntax ?? SyntaxFactory.Block())
            .WithLeadingTrivia(BuildDoc(
                $"Convenience helper to delete {model.Name} by primary key.",
                new[]
                {
                    ("id", $"Primary key value of the {model.Name} row to delete."),
                    ("args", "Optional args controlling select/include/omit."),
                    ("ct", "Cancellation token for the async operation.")
                },
                $"The deleted {model.Name} instance or null."));
    }

    private MethodDeclarationSyntax BuildUpdateBySinglePrimaryKey(CharismaSchema schema, ModelDefinition model, ScalarFieldDefinition pkField)
    {
        var idType = MapScalarType(schema, pkField.RawType, nullable: false);
        var bodyText = $@"{{
    var argsValue = args ?? new {model.Name}UpdateArgs
    {{
        Data = data,
        Where = new {model.Name}WhereUniqueInput {{ {pkField.Name} = id }}
    }};

    argsValue.Data = data;
    argsValue.Where ??= new {model.Name}WhereUniqueInput {{ {pkField.Name} = id }};
    return await UpdateAsync(argsValue, ct).ConfigureAwait(false);
}}";

        return SyntaxFactory.MethodDeclaration(SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"), "UpdateByIdAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("id")).WithType(SyntaxFactory.ParseTypeName(idType)),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("data")).WithType(SyntaxFactory.ParseTypeName($"{model.Name}UpdateInput")),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("args"))
                    .WithType(SyntaxFactory.ParseTypeName($"{model.Name}UpdateArgs?"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression))),
                SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
                    .WithType(SyntaxFactory.ParseTypeName("CancellationToken"))
                    .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.IdentifierName("default"))))
            .WithBody(SyntaxFactory.ParseStatement(bodyText) as BlockSyntax ?? SyntaxFactory.Block())
            .WithLeadingTrivia(BuildDoc(
                $"Convenience helper to update {model.Name} by primary key.",
                new[]
                {
                    ("id", $"Primary key value of the {model.Name} row to update."),
                    ("data", "Fields to update."),
                    ("args", "Optional args controlling select/include/omit."),
                    ("ct", "Cancellation token for the async operation.")
                },
                $"The updated {model.Name} instance or null."));
    }

    private MethodDeclarationSyntax BuildFindByCompositePrimaryKey(CharismaSchema schema, ModelDefinition model, CompositePrimaryKeySelector selector, IReadOnlyList<ScalarFieldDefinition> fields)
    {
        var body = BuildCompositePrimaryKeyBody(
            model,
            selector,
            fields,
            argsTypeName: $"{model.Name}FindUniqueArgs",
            whereAssignmentTarget: "argsValue.Where",
            setup: "",
            returnCall: "return await FindUniqueAsync(argsValue, ct).ConfigureAwait(false);");

        return SyntaxFactory.MethodDeclaration(SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"), "FindByKeyAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(BuildCompositePkParameters(schema, model, fields, includeData: false, argsTypeName: $"{model.Name}FindUniqueArgs?").ToArray())
            .WithBody(body)
            .WithLeadingTrivia(BuildDoc(
                $"Convenience helper to find {model.Name} by composite primary key.",
                BuildCompositePkParamDocs(fields, includeData: false),
                $"The matching {model.Name} instance or null."));
    }

    private MethodDeclarationSyntax BuildDeleteByCompositePrimaryKey(CharismaSchema schema, ModelDefinition model, CompositePrimaryKeySelector selector, IReadOnlyList<ScalarFieldDefinition> fields)
    {
        var body = BuildCompositePrimaryKeyBody(
            model,
            selector,
            fields,
            argsTypeName: $"{model.Name}DeleteArgs",
            whereAssignmentTarget: "argsValue.Where",
            setup: "",
            returnCall: "return await DeleteAsync(argsValue, ct).ConfigureAwait(false);");

        return SyntaxFactory.MethodDeclaration(SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"), "DeleteByKeyAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(BuildCompositePkParameters(schema, model, fields, includeData: false, argsTypeName: $"{model.Name}DeleteArgs?").ToArray())
            .WithBody(body)
            .WithLeadingTrivia(BuildDoc(
                $"Convenience helper to delete {model.Name} by composite primary key.",
                BuildCompositePkParamDocs(fields, includeData: false),
                $"The deleted {model.Name} instance or null."));
    }

    private MethodDeclarationSyntax BuildUpdateByCompositePrimaryKey(CharismaSchema schema, ModelDefinition model, CompositePrimaryKeySelector selector, IReadOnlyList<ScalarFieldDefinition> fields)
    {
        var body = BuildCompositePrimaryKeyBody(
            model,
            selector,
            fields,
            argsTypeName: $"{model.Name}UpdateArgs",
            whereAssignmentTarget: "argsValue.Where",
            setup: "argsValue.Data = data;",
            returnCall: "return await UpdateAsync(argsValue, ct).ConfigureAwait(false);");

        return SyntaxFactory.MethodDeclaration(SyntaxFactory.ParseTypeName($"Task<{model.Name}?>"), "UpdateByKeyAsync")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.AsyncKeyword))
            .AddParameterListParameters(BuildCompositePkParameters(schema, model, fields, includeData: true, argsTypeName: $"{model.Name}UpdateArgs?").ToArray())
            .WithBody(body)
            .WithLeadingTrivia(BuildDoc(
                $"Convenience helper to update {model.Name} by composite primary key.",
                BuildCompositePkParamDocs(fields, includeData: true),
                $"The updated {model.Name} instance or null."));
    }

    private BlockSyntax BuildCompositePrimaryKeyBody(
        ModelDefinition model,
        CompositePrimaryKeySelector selector,
        IReadOnlyList<ScalarFieldDefinition> fields,
        string argsTypeName,
        string whereAssignmentTarget,
        string setup,
        string returnCall)
    {
        var selectorAssignments = string.Join(", ", fields.Select(f => $"{f.Name} = {ToCamelCase(f.Name)}"));
        var setupSection = string.IsNullOrWhiteSpace(setup) ? string.Empty : $"\n    {setup}\n";
        var bodyText = $@"{{
    var argsValue = args ?? new {argsTypeName}
    {{
        Where = new {model.Name}WhereUniqueInput
        {{
            {selector.PropertyName} = new {selector.SelectorTypeName}
            {{
                {selectorAssignments}
            }}
        }}
    }};
{setupSection}
    {whereAssignmentTarget} ??= new {model.Name}WhereUniqueInput
    {{
        {selector.PropertyName} = new {selector.SelectorTypeName}
        {{
            {selectorAssignments}
        }}
    }};

    {returnCall}
}}";

        return SyntaxFactory.ParseStatement(bodyText) as BlockSyntax ?? SyntaxFactory.Block();
    }

    private IEnumerable<ParameterSyntax> BuildCompositePkParameters(CharismaSchema schema, ModelDefinition model, IReadOnlyList<ScalarFieldDefinition> fields, bool includeData, string argsTypeName)
    {
        foreach (var field in fields)
        {
            yield return SyntaxFactory.Parameter(SyntaxFactory.Identifier(ToCamelCase(field.Name)))
                .WithType(SyntaxFactory.ParseTypeName(MapScalarType(schema, field.RawType, nullable: false)));
        }

        if (includeData)
        {
            yield return SyntaxFactory.Parameter(SyntaxFactory.Identifier("data"))
                .WithType(SyntaxFactory.ParseTypeName($"{model.Name}UpdateInput"));
        }

        yield return SyntaxFactory.Parameter(SyntaxFactory.Identifier("args"))
            .WithType(SyntaxFactory.ParseTypeName(argsTypeName))
            .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)));

        yield return SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
            .WithType(SyntaxFactory.ParseTypeName("CancellationToken"))
            .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.IdentifierName("default")));
    }

    private static IEnumerable<(string Name, string Description)> BuildCompositePkParamDocs(IReadOnlyList<ScalarFieldDefinition> fields, bool includeData)
    {
        var docs = new List<(string Name, string Description)>();
        docs.AddRange(fields.Select(f => (ToCamelCase(f.Name), $"Primary key component '{f.Name}'.")));

        if (includeData)
        {
            docs.Add(("data", "Fields to update."));
        }

        docs.Add(("args", "Optional args controlling select/include/omit."));
        docs.Add(("ct", "Cancellation token for the async operation."));
        return docs;
    }

    private static CompositePrimaryKeySelector BuildCompositePrimaryKeySelector(ModelDefinition model, PrimaryKeyDefinition pk)
    {
        var propertyName = $"By{string.Join("And", pk.Fields.Select(ToPascalIdentifier))}";
        return new CompositePrimaryKeySelector(propertyName, $"{model.Name}{propertyName}Input");
    }

    private static string ToPascalIdentifier(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return "Key";
        }

        var parts = raw
            .Split(new[] { '_', '-', ' ', '.', ':', ';', '/', '\\', ',', '(', ')', '[', ']' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim())
            .Where(p => p.Length > 0)
            .ToArray();

        if (parts.Length == 0)
        {
            return "Key";
        }

        var pascal = string.Concat(parts.Select(p => char.ToUpperInvariant(p[0]) + p[1..]));
        if (!char.IsLetter(pascal[0]) && pascal[0] != '_')
        {
            pascal = $"K{pascal}";
        }

        return pascal;
    }

    private static string ToCamelCase(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return "key";
        }

        if (raw.Length == 1)
        {
            return raw.ToLowerInvariant();
        }

        return char.ToLowerInvariant(raw[0]) + raw[1..];
    }

    private sealed record CompositePrimaryKeySelector(string PropertyName, string SelectorTypeName);

    /// <summary>
    /// Composes a delegate method that constructs a query model and dispatches it through the executor.
    /// </summary>
    /// <param name="returnType">Declared return type.</param>
    /// <param name="name">Method name.</param>
    /// <param name="argsType">Argument type.</param>
    /// <param name="queryType">Query type enum.</param>
    /// <param name="modelName">Target model name.</param>
    /// <param name="argsOptional">Whether args are optional.</param>
    /// <param name="defaultArgsExpression">Default args initializer when optional.</param>
    /// <param name="nonQuery">True when the method returns affected rows instead of records.</param>
    /// <param name="isMany">True when returning multiple results.</param>
    /// <param name="returnRecords">True when records are returned (otherwise scalars/counts).</param>
    /// <param name="throwIfNotFound">Flag for FindFirstOrThrow behavior.</param>
    /// <param name="resultTypeOverride">Optional override for the generic result type.</param>
    /// <returns>Method declaration with documentation.</returns>
    private MethodDeclarationSyntax BuildDelegateMethod(
        TypeSyntax returnType,
        string name,
        TypeSyntax argsType,
        QueryType queryType,
        string modelName,
        bool argsOptional = false,
        ExpressionSyntax? defaultArgsExpression = null,
        bool nonQuery = false,
        bool isMany = false,
        bool returnRecords = true,
        bool throwIfNotFound = false,
        string? resultTypeOverride = null)
    {
        var ctParam = SyntaxFactory.Parameter(SyntaxFactory.Identifier("ct"))
            .WithType(SyntaxFactory.ParseTypeName("CancellationToken"))
            .WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.IdentifierName("default")));

        var argsParam = SyntaxFactory.Parameter(SyntaxFactory.Identifier("args"))
            .WithType(argsOptional ? SyntaxFactory.ParseTypeName($"{argsType}?") : argsType);

        if (argsOptional)
        {
            argsParam = argsParam.WithDefault(SyntaxFactory.EqualsValueClause(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)));
        }

        var returnTypeSingle = !isMany && !nonQuery;
        var body = BuildBody(modelName, queryType, argsOptional ? "argsValue" : "args", returnTypeSingle: returnTypeSingle, nonQuery: nonQuery, returnRecords: returnRecords, throwIfNotFound: throwIfNotFound, resultTypeOverride: resultTypeOverride);

        if (argsOptional)
        {
            var initStmt = SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(argsType)
                    .AddVariables(
                        SyntaxFactory.VariableDeclarator("argsValue")
                            .WithInitializer(
                                SyntaxFactory.EqualsValueClause(
                                    SyntaxFactory.BinaryExpression(
                                        SyntaxKind.CoalesceExpression,
                                        SyntaxFactory.IdentifierName("args"),
                                        defaultArgsExpression ?? SyntaxFactory.ObjectCreationExpression(argsType).WithArgumentList(SyntaxFactory.ArgumentList()))))));
            body = body.WithStatements(body.Statements.Insert(0, initStmt));
        }

        var summary = nonQuery
            ? $"Executes {modelName}.{name.Replace("Async", string.Empty)} as a non-query operation."
            : isMany
                ? $"Executes {modelName}.{name.Replace("Async", string.Empty)} returning multiple results."
                : $"Executes {modelName}.{name.Replace("Async", string.Empty)} returning a single result.";

        var paramDocs = new List<(string, string)>
        {
            ("args", argsOptional ? "Optional arguments for the query." : "Arguments for the query."),
            ("ct", "Cancellation token for the async operation.")
        };

        var returnsDoc = nonQuery
            ? "Affected row count."
            : isMany
                ? $"A list of {modelName} results."
                : returnRecords
                    ? (throwIfNotFound
                        ? $"A single {modelName} result; throws if no matching record is found."
                        : $"A single {modelName} result or null.")
                    : "Scalar result.";

        var method = SyntaxFactory.MethodDeclaration(returnType, name)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddParameterListParameters(argsParam, ctParam)
            .WithBody(body)
            .WithLeadingTrivia(BuildDoc(summary, paramDocs, returnsDoc));

        if (throwIfNotFound)
        {
            // Mark method async so we can await the executor and perform null-checks.
            method = method.AddModifiers(SyntaxFactory.Token(SyntaxKind.AsyncKeyword));
        }

        return method;
    }

    /// <summary>
    /// Builds the method body that instantiates the query model and calls the executor.
    /// </summary>
    /// <param name="modelName">Target model name.</param>
    /// <param name="queryType">Query type.</param>
    /// <param name="argsIdentifier">Identifier of the args variable.</param>
    /// <param name="returnTypeSingle">True when a single record is expected.</param>
    /// <param name="nonQuery">True when this is a non-query mutation.</param>
    /// <param name="returnRecords">True when records should be returned.</param>
    /// <param name="throwIfNotFound">Flag for FindFirst throw semantics.</param>
    /// <param name="resultTypeOverride">Optional override for generic result type.</param>
    /// <returns>Block syntax for the method body.</returns>
    private static BlockSyntax BuildBody(
   string modelName,
   QueryType queryType,
   string argsIdentifier,
    bool returnTypeSingle,
    bool nonQuery,
    bool returnRecords,
    bool throwIfNotFound,
    string? resultTypeOverride)
    {
        // var model = new TypedQueryModel("Model", argsIdentifier);
        var modelTypeName = queryType switch
        {
            QueryType.FindUnique => "FindUniqueQueryModel",
            QueryType.FindFirst => "FindFirstQueryModel",
            QueryType.FindMany => "FindManyQueryModel",
            QueryType.Create => "CreateQueryModel",
            QueryType.CreateMany => "CreateManyQueryModel",
            QueryType.Update => "UpdateQueryModel",
            QueryType.UpdateMany => "UpdateManyQueryModel",
            QueryType.Upsert => "UpsertQueryModel",
            QueryType.Delete => "DeleteQueryModel",
            QueryType.DeleteMany => "DeleteManyQueryModel",
            QueryType.Count => "CountQueryModel",
            QueryType.Aggregate => "AggregateQueryModel",
            QueryType.GroupBy => "GroupByQueryModel",
            _ => throw new InvalidOperationException($"Unsupported query type: {queryType}")
        };

        var modelDecl = SyntaxFactory.LocalDeclarationStatement(
            SyntaxFactory.VariableDeclaration(SyntaxFactory.IdentifierName("var"))
                .AddVariables(
                    SyntaxFactory.VariableDeclarator("model")
                        .WithInitializer(
                            SyntaxFactory.EqualsValueClause(
                                SyntaxFactory.ObjectCreationExpression(SyntaxFactory.IdentifierName(modelTypeName))
                                    .WithArgumentList(
                                        SyntaxFactory.ArgumentList(
                                            SyntaxFactory.SeparatedList(GetModelArguments(modelName, queryType, argsIdentifier, returnRecords, throwIfNotFound))))))));

        // Handle executor invocation. If throwIfNotFound is set for a single-record query,
        // generate an async flow that awaits the executor, checks for null, throws, and returns.
        if (throwIfNotFound && !nonQuery && returnTypeSingle && returnRecords)
        {
            // Build: var result = await _executor.ExecuteSingleAsync<Model>(model, null, ct);
            var typeArg = resultTypeOverride is not null
                ? SyntaxFactory.IdentifierName(resultTypeOverride)
                : SyntaxFactory.IdentifierName(modelName);

            var execInvocation = SyntaxFactory.AwaitExpression(
                SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("_executor"),
                        SyntaxFactory.GenericName("ExecuteSingleAsync")
                            .WithTypeArgumentList(
                                SyntaxFactory.TypeArgumentList(
                                    SyntaxFactory.SingletonSeparatedList<TypeSyntax>(
                                        typeArg)))))
                    .WithArgumentList(
                        SyntaxFactory.ArgumentList(
                            SyntaxFactory.SeparatedList(new[]
                            {
                                SyntaxFactory.Argument(SyntaxFactory.IdentifierName("model")),
                                SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)),
                                SyntaxFactory.Argument(SyntaxFactory.IdentifierName("ct"))
                            }))));

            var resultDecl = SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(SyntaxFactory.IdentifierName("var"))
                    .AddVariables(
                        SyntaxFactory.VariableDeclarator("result")
                            .WithInitializer(SyntaxFactory.EqualsValueClause(execInvocation))));

            // if (result is null) throw new InvalidOperationException("No <Model> found.");
            var ifStmt = SyntaxFactory.IfStatement(
                SyntaxFactory.BinaryExpression(
                    SyntaxKind.EqualsExpression,
                    SyntaxFactory.IdentifierName("result"),
                    SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)),
                SyntaxFactory.ThrowStatement(
                    SyntaxFactory.ObjectCreationExpression(SyntaxFactory.IdentifierName("InvalidOperationException"))
                        .WithArgumentList(
                            SyntaxFactory.ArgumentList(
                                SyntaxFactory.SingletonSeparatedList(
                                    SyntaxFactory.Argument(
                                        SyntaxFactory.LiteralExpression(
                                            SyntaxKind.StringLiteralExpression,
                                            SyntaxFactory.Literal($"No {modelName} record found."))))))));

            var returnStmt = SyntaxFactory.ReturnStatement(SyntaxFactory.IdentifierName("result"));

            return SyntaxFactory.Block(modelDecl, resultDecl, ifStmt, returnStmt);
        }

        // Fallback: previous behavior (no special throw handling)
        ExpressionSyntax call;
        if (nonQuery)
        {
            call = SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("_executor"),
                        SyntaxFactory.IdentifierName("ExecuteNonQueryAsync")))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SeparatedList(new[]
                        {
                            SyntaxFactory.Argument(SyntaxFactory.IdentifierName("model")),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)),
                            SyntaxFactory.Argument(SyntaxFactory.IdentifierName("ct"))
                        })));
        }
        else if (!returnTypeSingle)
        {
            var typeArg = resultTypeOverride is not null
                ? SyntaxFactory.IdentifierName(resultTypeOverride)
                : returnRecords
                    ? SyntaxFactory.IdentifierName(modelName)
                    : SyntaxFactory.IdentifierName("int");
            call = SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("_executor"),
                        SyntaxFactory.GenericName("ExecuteManyAsync")
                            .WithTypeArgumentList(
                                SyntaxFactory.TypeArgumentList(
                                    SyntaxFactory.SingletonSeparatedList<TypeSyntax>(
                                        typeArg)))))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SeparatedList(new[]
                        {
                            SyntaxFactory.Argument(SyntaxFactory.IdentifierName("model")),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)),
                            SyntaxFactory.Argument(SyntaxFactory.IdentifierName("ct"))
                        })));
        }
        else
        {
            var typeArg = resultTypeOverride is not null
                ? SyntaxFactory.IdentifierName(resultTypeOverride)
                : returnRecords
                    ? SyntaxFactory.IdentifierName(modelName)
                    : SyntaxFactory.IdentifierName("int");
            call = SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("_executor"),
                        SyntaxFactory.GenericName("ExecuteSingleAsync")
                            .WithTypeArgumentList(
                                SyntaxFactory.TypeArgumentList(
                                    SyntaxFactory.SingletonSeparatedList<TypeSyntax>(
                                        typeArg)))))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SeparatedList(new[]
                        {
                            SyntaxFactory.Argument(SyntaxFactory.IdentifierName("model")),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)),
                            SyntaxFactory.Argument(SyntaxFactory.IdentifierName("ct"))
                        })));
        }

        var returnStmtFallback = SyntaxFactory.ReturnStatement(call);

        return SyntaxFactory.Block(modelDecl, returnStmtFallback);
    }

    /// <summary>
    /// Emits constructor arguments for query model creation based on flags and query type.
    /// </summary>
    /// <param name="modelName">Target model name.</param>
    /// <param name="queryType">Query type enum.</param>
    /// <param name="argsIdentifier">Identifier for the args variable.</param>
    /// <param name="returnRecords">Whether records should be returned.</param>
    /// <param name="throwIfNotFound">Throw-if-not-found flag for FindFirst.</param>
    /// <returns>Sequence of argument syntax nodes.</returns>
    private static IEnumerable<ArgumentSyntax> GetModelArguments(string modelName, QueryType queryType, string argsIdentifier, bool returnRecords, bool throwIfNotFound)
    {
        yield return SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(modelName)));
        yield return SyntaxFactory.Argument(SyntaxFactory.IdentifierName(argsIdentifier));

        if (queryType == QueryType.CreateMany || queryType == QueryType.UpdateMany)
        {
            var literal = returnRecords
                ? SyntaxKind.TrueLiteralExpression
                : SyntaxKind.FalseLiteralExpression;
            yield return SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(literal));
        }

        if (queryType == QueryType.FindFirst && throwIfNotFound)
        {
            yield return SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.TrueLiteralExpression));
        }
    }

    /// <summary>
    /// Maps a schema scalar type to a C# type name.
    /// </summary>
    /// <param name="schema">Schema context.</param>
    /// <param name="rawType">Raw scalar type from schema.</param>
    /// <param name="nullable">Whether the result should be nullable.</param>
    /// <returns>C# type name string.</returns>
    private static string MapScalarType(CharismaSchema schema, string rawType, bool nullable)
    {
        string typeName = rawType switch
        {
            "String" => "string",
            "Int" => "int",
            "Float" => "double",
            "Decimal" => "decimal",
            "DateTime" => "DateTime",
            "Boolean" => "bool",
            "UUID" or "Id" => "Guid",
            "Bytes" => "byte[]",
            "Json" => "Json",
            _ when schema.Enums.ContainsKey(rawType) => rawType,
            _ => rawType
        };

        if (nullable && typeName != "string" && typeName != "byte[]")
        {
            typeName += "?";
        }

        if (nullable && (typeName == "string" || typeName == "byte[]"))
        {
            typeName += "?";
        }

        return typeName;
    }

    /// <summary>
    /// Builds XML documentation trivia for a generated member.
    /// </summary>
    /// <param name="summary">Summary text.</param>
    /// <param name="parameters">Optional parameter docs.</param>
    /// <param name="returns">Optional returns doc.</param>
    /// <returns>Leading trivia representing the XML doc.</returns>
    private static SyntaxTriviaList BuildDoc(string summary, IEnumerable<(string Name, string Description)>? parameters = null, string? returns = null)
    {
        var sb = new System.Text.StringBuilder();
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