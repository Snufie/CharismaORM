using Charisma.Schema;
using System.Text.Json.Serialization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates C# enums from schema enums.
/// </summary>
internal sealed class EnumWriter : IWriter
{
    private readonly string _rootNamespace;

    public EnumWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Generates a compilation unit per schema enum, mapping to JsonStringEnumConverter-backed C# enums.
    /// </summary>
    /// <param name="schema">Schema containing enums to emit.</param>
    /// <returns>Compilation units, one per enum.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var units = new List<CompilationUnitSyntax>();

        var enumDefinitions = new List<EnumDefinition>(schema.Enums.Values);
        Console.WriteLine($"Generating {enumDefinitions.Count} enums.");
        enumDefinitions.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        foreach (var enumDef in enumDefinitions)
        {
            Console.WriteLine($"Generating enum: {enumDef.Name}");
            var members = new List<EnumMemberDeclarationSyntax>();
            foreach (var value in enumDef.Values)
            {
                members.Add(SyntaxFactory.EnumMemberDeclaration(SyntaxFactory.Identifier(value)));
            }
            Console.WriteLine($"  with {members.Count} members.");

            var @enum = SyntaxFactory.EnumDeclaration(enumDef.Name)
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                .AddAttributeLists(
                    SyntaxFactory.AttributeList(
                        SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.Attribute(
                                SyntaxFactory.IdentifierName("JsonConverter"))
                            .WithArgumentList(
                                SyntaxFactory.AttributeArgumentList(
                                    SyntaxFactory.SingletonSeparatedList(
                                        SyntaxFactory.AttributeArgument(
                                            SyntaxFactory.TypeOfExpression(
                                                SyntaxFactory.IdentifierName("JsonStringEnumConverter")))))))))
                .AddMembers(members.ToArray());

            var @namespace = SyntaxFactory.NamespaceDeclaration(
                    SyntaxFactory.ParseName($"{_rootNamespace}.Enums"))
                .AddMembers(@enum);

            var unit = SyntaxFactory.CompilationUnit()
                .AddUsings(
                    SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Text.Json.Serialization")))
                .AddMembers(@namespace)
                .NormalizeWhitespace();

            units.Add(unit);
        }

        return units.AsReadOnly();
    }
}