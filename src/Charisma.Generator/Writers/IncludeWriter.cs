using Charisma.Schema;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates Include types per model.
/// </summary>
internal sealed class IncludeWriter : IWriter
{
    private readonly string _rootNamespace;

    public IncludeWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Emits Include classes per model capturing relation include graphs plus optional select/omit masks.
    /// </summary>
    /// <param name="schema">Schema containing models to generate Include shapes for.</param>
    /// <returns>Compilation units, one per model Include class.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var units = new List<CompilationUnitSyntax>();
        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        foreach (var model in models)
        {
            units.Add(BuildIncludeUnit(model));
        }

        return units.AsReadOnly();
    }

    /// <summary>
    /// Builds a single model's Include class with relation includes and select/omit masks.
    /// </summary>
    /// <param name="model">Model to generate include shape for.</param>
    private CompilationUnitSyntax BuildIncludeUnit(ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        var fields = new List<FieldDefinition>(model.Fields);
        fields.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        props.Add(BuildSpecialProp($"{model.Name}Select?", "Select", "Optional select mask for the included model (scalars only)."));
        props.Add(BuildSpecialProp($"{model.Name}Omit?", "Omit", "Optional omit mask for the included model (scalars only)."));

        foreach (var field in fields)
        {
            if (field is RelationFieldDefinition relation)
            {
                props.Add(BuildProp($"{relation.RawType}Include?", field.Name));
            }
        }

        var classDecl = SyntaxFactory.ClassDeclaration($"{model.Name}Include")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.PartialKeyword))
            .WithLeadingTrivia(BuildDoc($"Relation include graph for {model.Name}. Mutually exclusive with Select and Omit."))
            .AddMembers(props.ToArray());

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Include"))
            .AddMembers(classDecl);

        return SyntaxFactory.CompilationUnit()
            .AddUsings(
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Select")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Omit")))
            .AddMembers(@namespace)
            .NormalizeWhitespace();
    }

    /// <summary>
    /// Builds special select/omit properties with custom summaries.
    /// </summary>
    /// <param name="typeName">Property type name.</param>
    /// <param name="propName">Property identifier.</param>
    /// <param name="summary">Summary text for XML docs.</param>
    private static PropertyDeclarationSyntax BuildSpecialProp(string typeName, string propName, string summary)
    {
        var prop = SyntaxFactory.PropertyDeclaration(SyntaxFactory.ParseTypeName(typeName), propName)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)));

        return prop.WithLeadingTrivia(BuildDoc(summary));
    }

    /// <summary>
    /// Builds relation include property with default doc text.
    /// </summary>
    /// <param name="typeName">Property type name.</param>
    /// <param name="propName">Property identifier.</param>
    private static PropertyDeclarationSyntax BuildProp(string typeName, string propName)
    {
        var prop = SyntaxFactory.PropertyDeclaration(SyntaxFactory.ParseTypeName(typeName), propName)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)));

        return prop.WithLeadingTrivia(BuildDoc($"Include relation '{propName}' and its own select/include graph."));
    }

    /// <summary>
    /// Creates minimal XML doc trivia for summary text.
    /// </summary>
    private static SyntaxTriviaList BuildDoc(string summary)
    {
        return SyntaxFactory.ParseLeadingTrivia($"/// <summary>{summary}</summary>\n");
    }
}