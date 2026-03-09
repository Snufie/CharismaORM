using Charisma.Schema;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates Omit types per model.
/// </summary>
internal sealed class OmitWriter : IWriter
{
    private readonly string _rootNamespace;

    public OmitWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Emits per-model omit masks and a global defaults container.
    /// </summary>
    /// <param name="schema">Schema containing models to generate omit shapes for.</param>
    /// <returns>Compilation units for model omits and global options.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var units = new List<CompilationUnitSyntax>();
        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        foreach (var model in models)
        {
            var props = new List<PropertyDeclarationSyntax>();
            var fields = new List<FieldDefinition>(model.Fields);
            fields.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

            foreach (var field in fields)
            {
                if (field is ScalarFieldDefinition)
                {
                    props.Add(BuildProp("bool?", field.Name));
                }
            }

            var classDecl = SyntaxFactory.ClassDeclaration($"{model.Name}Omit")
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.PartialKeyword))
                .WithLeadingTrivia(BuildDoc($"Field omission mask for {model.Name}. Mutually exclusive with Select and Include."))
                .AddMembers(props.ToArray());

            var @namespace = SyntaxFactory.NamespaceDeclaration(
                    SyntaxFactory.ParseName($"{_rootNamespace}.Omit"))
                .AddMembers(classDecl);

            var unit = SyntaxFactory.CompilationUnit()
                .AddMembers(@namespace)
                .NormalizeWhitespace();

            units.Add(unit);
        }

        units.Add(BuildGlobalOmitOptions(models));

        return units.AsReadOnly();
    }

    /// <summary>
    /// Builds the GlobalOmitOptions type aggregating default omits per model.
    /// </summary>
    /// <param name="models">Models to expose on the options type.</param>
    private CompilationUnitSyntax BuildGlobalOmitOptions(IReadOnlyList<ModelDefinition> models)
    {
        var props = new List<PropertyDeclarationSyntax>();
        foreach (var model in models)
        {
            props.Add(BuildModelOmitProp(model.Name));
        }

        var classDecl = SyntaxFactory.ClassDeclaration("GlobalOmitOptions")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .WithLeadingTrivia(BuildDoc("Default omit masks keyed by model."))
            .AddMembers(props.ToArray())
            .AddMembers(BuildToDictionaryMethod(models));

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Omit"))
            .AddMembers(classDecl);

        return SyntaxFactory.CompilationUnit()
            .AddUsings(SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Collections.Generic")))
            .AddMembers(@namespace)
            .NormalizeWhitespace();
    }

    /// <summary>
    /// Builds ToDictionary to materialize omits for runtime consumption.
    /// </summary>
    /// <param name="models">Models to include in the dictionary.</param>
    private static MemberDeclarationSyntax BuildToDictionaryMethod(IReadOnlyList<ModelDefinition> models)
    {
        var statements = new List<StatementSyntax>
        {
            SyntaxFactory.ParseStatement("var map = new Dictionary<string, object?>();")
        };

        foreach (var model in models)
        {
            var name = model.Name;
            statements.Add(
                SyntaxFactory.ParseStatement($"if ({name} is not null) map.Add(\"{name}\", {name});"));
        }

        statements.Add(SyntaxFactory.ParseStatement("return map;"));

        return SyntaxFactory.MethodDeclaration(
                SyntaxFactory.ParseTypeName("IReadOnlyDictionary<string, object?>"),
                "ToDictionary")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .WithBody(SyntaxFactory.Block(statements))
            .WithLeadingTrivia(BuildDoc("Materializes a dictionary for runtime consumption."));
    }

    /// <summary>
    /// Builds a nullable model-specific omit property.
    /// </summary>
    /// <param name="modelName">Model name used for type and property.</param>
    private static PropertyDeclarationSyntax BuildModelOmitProp(string modelName)
    {
        var typeName = $"{modelName}Omit?";
        var prop = SyntaxFactory.PropertyDeclaration(SyntaxFactory.ParseTypeName(typeName), modelName)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.InitAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)));

        return prop.WithLeadingTrivia(BuildDoc($"Default omit for {modelName} fields."));
    }

    /// <summary>
    /// Builds a scalar field omit boolean property.
    /// </summary>
    /// <param name="typeName">Property type name.</param>
    /// <param name="propName">Scalar field name.</param>
    private static PropertyDeclarationSyntax BuildProp(string typeName, string propName)
    {
        var prop = SyntaxFactory.PropertyDeclaration(SyntaxFactory.ParseTypeName(typeName), propName)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)));

        return prop.WithLeadingTrivia(BuildDoc($"Exclude scalar field '{propName}'."));
    }

    /// <summary>
    /// Creates minimal XML doc trivia for summaries.
    /// </summary>
    private static SyntaxTriviaList BuildDoc(string summary)
    {
        return SyntaxFactory.ParseLeadingTrivia($"/// <summary>{summary}</summary>\n");
    }
}
