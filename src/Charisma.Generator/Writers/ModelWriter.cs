using Charisma.Schema;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates partial POCO classes for each model.
/// </summary>
internal sealed class ModelWriter : IWriter
{
    private readonly string _rootNamespace;

    public ModelWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Generates partial POCO classes for every schema model with JSON ignore semantics for omitted fields.
    /// </summary>
    /// <param name="schema">Schema containing models to emit.</param>
    /// <returns>Compilation units for each model class.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var units = new List<CompilationUnitSyntax>();

        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        foreach (var model in models)
        {
            var classDeclaration = SyntaxFactory.ClassDeclaration(model.Name)
                .AddModifiers(
                    SyntaxFactory.Token(SyntaxKind.PublicKeyword),
                    SyntaxFactory.Token(SyntaxKind.PartialKeyword))
                .WithLeadingTrivia(BuildDoc($"Generated POCO for model {model.Name}."));

            var fields = new List<FieldDefinition>(model.Fields);
            fields.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

            foreach (var field in fields)
            {
                var (typeSyntax, isReferenceType, isRelation) = CreateTypeSyntax(field, schema);

                var property = SyntaxFactory.PropertyDeclaration(typeSyntax, field.Name)
                    .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                    .AddAccessorListAccessors(
                        SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration)
                            .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                        SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration)
                            .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)))
                    .WithLeadingTrivia(BuildDoc(BuildFieldSummary(field)));

                // Hide default/unset values from serialization so non-selected/non-included columns don’t emit defaults.
                if (isRelation)
                {
                    property = property.AddAttributeLists(
                        SyntaxFactory.AttributeList(
                            SyntaxFactory.SingletonSeparatedList(
                                SyntaxFactory.Attribute(
                                    SyntaxFactory.IdentifierName("JsonIgnore"))
                                .WithArgumentList(
                                    SyntaxFactory.AttributeArgumentList(
                                        SyntaxFactory.SingletonSeparatedList(
                                            SyntaxFactory.AttributeArgument(
                                                SyntaxFactory.NameEquals("Condition"),
                                                null,
                                                SyntaxFactory.MemberAccessExpression(
                                                    SyntaxKind.SimpleMemberAccessExpression,
                                                    SyntaxFactory.IdentifierName("JsonIgnoreCondition"),
                                                    SyntaxFactory.IdentifierName("WhenWritingNull")))))))));
                }
                else
                {
                    property = property.AddAttributeLists(
                        SyntaxFactory.AttributeList(
                            SyntaxFactory.SingletonSeparatedList(
                                SyntaxFactory.Attribute(
                                    SyntaxFactory.IdentifierName("JsonIgnore"))
                                .WithArgumentList(
                                    SyntaxFactory.AttributeArgumentList(
                                        SyntaxFactory.SingletonSeparatedList(
                                            SyntaxFactory.AttributeArgument(
                                                SyntaxFactory.NameEquals("Condition"),
                                                null,
                                                SyntaxFactory.MemberAccessExpression(
                                                    SyntaxKind.SimpleMemberAccessExpression,
                                                    SyntaxFactory.IdentifierName("JsonIgnoreCondition"),
                                                    SyntaxFactory.IdentifierName("WhenWritingDefault")))))))));
                }

                if (isReferenceType)
                {
                    property = property.WithInitializer(
                        SyntaxFactory.EqualsValueClause(
                            SyntaxFactory.PostfixUnaryExpression(
                                SyntaxKind.SuppressNullableWarningExpression,
                                SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression))))
                        .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken));
                }

                classDeclaration = classDeclaration.AddMembers(property);
            }

            var @namespace = SyntaxFactory.NamespaceDeclaration(
                    SyntaxFactory.ParseName($"{_rootNamespace}.Models"))
                .AddMembers(classDeclaration);

            var usings = new List<UsingDirectiveSyntax>
            {
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Collections.Generic")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Text.Json")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Text.Json.Serialization")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Runtime"))
            };

            if (schema.Enums.Count > 0)
            {
                usings.Insert(usings.Count - 1, SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Enums")));
            }

            var unit = SyntaxFactory.CompilationUnit()
                .AddUsings(usings.ToArray())
                .AddMembers(@namespace)
                .NormalizeWhitespace();

            units.Add(unit);
        }

        return units.AsReadOnly();
    }

    /// <summary>
    /// Maps a schema field to its type syntax, including list/nullable handling and relation flag.
    /// </summary>
    /// <param name="field">Field to map.</param>
    /// <param name="schema">Schema used for enum/scalar resolution.</param>
    /// <returns>Type syntax, reference-type indicator, and relation indicator.</returns>
    private static (TypeSyntax Type, bool IsReferenceType, bool IsRelation) CreateTypeSyntax(FieldDefinition field, CharismaSchema schema)
    {
        bool isList = field.IsList;
        bool isOptional = field.IsOptional;
        bool isRelation = field is RelationFieldDefinition;

        (TypeSyntax baseType, bool isValueType) = field switch
        {
            ScalarFieldDefinition scalar => MapScalarType(scalar.RawType, schema),
            RelationFieldDefinition relation => (SyntaxFactory.IdentifierName(relation.RawType), false),
            _ => throw new InvalidOperationException($"Unsupported field type for '{field.Name}'.")
        };

        if (isList)
        {
            return (SyntaxFactory.GenericName("IReadOnlyList")
                .WithTypeArgumentList(
                    SyntaxFactory.TypeArgumentList(
                        SyntaxFactory.SingletonSeparatedList(baseType))), true, isRelation);
        }

        if (isOptional)
        {
            return (SyntaxFactory.NullableType(baseType), false, isRelation);
        }

        return (baseType, !isValueType, isRelation);
    }

    /// <summary>
    /// Maps schema scalar types (and enums) to C# type syntax and value-type flag.
    /// </summary>
    /// <param name="rawType">Schema scalar type name.</param>
    /// <param name="schema">Schema for enum detection.</param>
    /// <returns>Type syntax and whether it is a value type.</returns>
    private static (TypeSyntax Type, bool IsValueType) MapScalarType(string rawType, CharismaSchema schema)
    {
        // Enums are value types.
        if (schema.Enums.ContainsKey(rawType))
        {
            return (SyntaxFactory.IdentifierName(rawType), true);
        }

        return rawType switch
        {
            "String" => (SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.StringKeyword)), false),
            "Int" => (SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.IntKeyword)), true),
            "Boolean" => (SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.BoolKeyword)), true),
            "Float" => (SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.DoubleKeyword)), true),
            "Decimal" => (SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.DecimalKeyword)), true),
            "DateTime" => (SyntaxFactory.IdentifierName("DateTime"), true),
            "Json" => (SyntaxFactory.IdentifierName("Json"), true),
            "Bytes" => (
                SyntaxFactory.ArrayType(
                    SyntaxFactory.PredefinedType(SyntaxFactory.Token(SyntaxKind.ByteKeyword)),
                    SyntaxFactory.SingletonList(
                        SyntaxFactory.ArrayRankSpecifier(
                            SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                                SyntaxFactory.OmittedArraySizeExpression())))),
                false),
            "UUID" or "Id" => (SyntaxFactory.IdentifierName("Guid"), true),
            _ => throw new InvalidOperationException($"Unsupported scalar type '{rawType}'.")
        };
    }

    /// <summary>
    /// Builds a short summary describing the field for XML docs.
    /// </summary>
    private static string BuildFieldSummary(FieldDefinition field)
    {
        return field switch
        {
            RelationFieldDefinition relation when relation.IsList => $"Related collection of {relation.RawType} records.",
            RelationFieldDefinition relation => $"Related {relation.RawType} record.",
            ScalarFieldDefinition => $"Scalar field '{field.Name}'.",
            _ => $"Field '{field.Name}'."
        };
    }

    /// <summary>
    /// Creates minimal XML doc trivia with a summary.
    /// </summary>
    private static SyntaxTriviaList BuildDoc(string summary)
    {
        return SyntaxFactory.ParseLeadingTrivia($"/// <summary>{summary}</summary>\n");
    }

}