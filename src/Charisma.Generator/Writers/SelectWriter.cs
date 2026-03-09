using Charisma.Schema;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates Select types per model.
/// </summary>
internal sealed class SelectWriter : IWriter
{
    private readonly string _rootNamespace;

    public SelectWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Emits per-model select masks, including nested selects for relations.
    /// </summary>
    /// <param name="schema">Schema containing models to generate.</param>
    /// <returns>Compilation units for each model's Select type.</returns>
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
                else if (field is RelationFieldDefinition relation)
                {
                    props.Add(BuildProp($"{relation.RawType}Select?", field.Name));
                }
            }

            var classDecl = SyntaxFactory.ClassDeclaration($"{model.Name}Select")
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.PartialKeyword))
                .WithLeadingTrivia(BuildDoc($"Field selection mask for {model.Name}. Mutually exclusive with Include and Omit."))
                .AddMembers(props.ToArray());

            var @namespace = SyntaxFactory.NamespaceDeclaration(
                    SyntaxFactory.ParseName($"{_rootNamespace}.Select"))
                .AddMembers(classDecl);

            var unit = SyntaxFactory.CompilationUnit()
                .AddMembers(@namespace)
                .NormalizeWhitespace();

            units.Add(unit);
        }

        return units.AsReadOnly();
    }

    /// <summary>
    /// Builds a select or nested select property with an explanatory summary.
    /// </summary>
    /// <param name="typeName">Property type name (bool? for scalars, nested select for relations).</param>
    /// <param name="propName">Field or relation name.</param>
    private static PropertyDeclarationSyntax BuildProp(string typeName, string propName)
    {
        var prop = SyntaxFactory.PropertyDeclaration(SyntaxFactory.ParseTypeName(typeName), propName)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)));

        var summary = typeName == "bool?"
            ? $"Include scalar field '{propName}'."
            : $"Nested select for relation '{propName}'.";

        return prop.WithLeadingTrivia(BuildDoc(summary));
    }

    /// <summary>
    /// Creates minimal XML doc trivia with a summary.
    /// </summary>
    private static SyntaxTriviaList BuildDoc(string summary)
    {
        return SyntaxFactory.ParseLeadingTrivia($"/// <summary>{summary}</summary>\n");
    }
}