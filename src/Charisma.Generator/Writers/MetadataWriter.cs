using System;
using System.Linq;
using Charisma.Schema;
using Charisma.QueryEngine.Metadata;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates a frozen metadata registry that describes PKs, uniques, indexes, and FKs for all models.
/// Runtime consumers (QueryEngine) can read this instead of using reflection or the parser.
/// </summary>
internal sealed class MetadataWriter : IWriter
{
    private readonly string _rootNamespace;

    public MetadataWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Generates a static registry describing models (PKs, uniques, indexes, FKs, fields) for runtime consumers.
    /// </summary>
    /// <param name="schema">Schema source for models and field metadata.</param>
    /// <returns>Compilation unit containing ModelMetadataRegistry.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        var registryClass = BuildRegistryClass(models, schema);

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Metadata"))
            .AddMembers(registryClass);

        var unit = SyntaxFactory.CompilationUnit()
            .AddUsings(
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Collections.Generic")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.QueryEngine.Metadata")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Schema")))
            .AddMembers(@namespace)
            .NormalizeWhitespace();

        return new[] { unit };
    }

    /// <summary>
    /// Builds the static ModelMetadataRegistry class exposing the metadata dictionary.
    /// </summary>
    /// <param name="models">Sorted models to include in the registry.</param>
    /// <param name="schema">Schema for type mapping.</param>
    private static ClassDeclarationSyntax BuildRegistryClass(IReadOnlyList<ModelDefinition> models, CharismaSchema schema)
    {
        var dictInitializerExpressions = new List<ExpressionSyntax>();
        foreach (var model in models)
        {
            dictInitializerExpressions.Add(
                SyntaxFactory.InitializerExpression(
                    SyntaxKind.ComplexElementInitializerExpression,
                    SyntaxFactory.SeparatedList<ExpressionSyntax>(new SyntaxNodeOrToken[]
                    {
                        SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(model.Name)),
                        SyntaxFactory.Token(SyntaxKind.CommaToken),
                        BuildModelMetadataCreation(model, schema)
                    })));
        }

        var dictionaryCreation = SyntaxFactory.ObjectCreationExpression(
                SyntaxFactory.ParseTypeName("Dictionary<string, ModelMetadata>"))
            .WithArgumentList(
                SyntaxFactory.ArgumentList(
                    SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.Argument(
                            SyntaxFactory.MemberAccessExpression(
                                SyntaxKind.SimpleMemberAccessExpression,
                                SyntaxFactory.IdentifierName("StringComparer"),
                                SyntaxFactory.IdentifierName("Ordinal")))))
            )
            .WithInitializer(
                SyntaxFactory.InitializerExpression(
                    SyntaxKind.CollectionInitializerExpression,
                    SyntaxFactory.SeparatedList(dictInitializerExpressions)));

        var property = SyntaxFactory.PropertyDeclaration(
                SyntaxFactory.ParseTypeName("IReadOnlyDictionary<string, ModelMetadata>"),
                SyntaxFactory.Identifier("All"))
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.StaticKeyword))
            .WithExpressionBody(
                SyntaxFactory.ArrowExpressionClause(dictionaryCreation))
            .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken));

        return SyntaxFactory.ClassDeclaration("ModelMetadataRegistry")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.StaticKeyword))
            .AddMembers(property);
    }

    /// <summary>
    /// Builds a ModelMetadata construction expression for a model.
    /// </summary>
    /// <param name="model">Model to describe.</param>
    /// <param name="schema">Schema for mapping field types.</param>
    private static ObjectCreationExpressionSyntax BuildModelMetadataCreation(ModelDefinition model, CharismaSchema schema)
    {
        var pkArg = model.PrimaryKey is null
            ? SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)
            : BuildPrimaryKeyMetadata(model.PrimaryKey);

        var uniqueArg = BuildUniqueConstraintsArray(model.UniqueConstraints);
        var indexArg = BuildIndexesArray(model.Indexes);
        var fkArg = BuildForeignKeysArray(model, schema);
        var fieldArg = BuildFieldsArray(model, schema);

        return SyntaxFactory.ObjectCreationExpression(
                SyntaxFactory.ParseTypeName("ModelMetadata"))
            .WithArgumentList(
                SyntaxFactory.ArgumentList(
                    SyntaxFactory.SeparatedList<ArgumentSyntax>(new SyntaxNodeOrToken[]
                    {
                        SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(model.Name))),
                        SyntaxFactory.Token(SyntaxKind.CommaToken),
                        SyntaxFactory.Argument(pkArg),
                        SyntaxFactory.Token(SyntaxKind.CommaToken),
                        SyntaxFactory.Argument(uniqueArg),
                        SyntaxFactory.Token(SyntaxKind.CommaToken),
                        SyntaxFactory.Argument(indexArg),
                        SyntaxFactory.Token(SyntaxKind.CommaToken),
                        SyntaxFactory.Argument(fkArg),
                        SyntaxFactory.Token(SyntaxKind.CommaToken),
                        SyntaxFactory.Argument(fieldArg)
                    })));
    }

    /// <summary>
    /// Builds FieldMetadata array for a model's fields.
    /// </summary>
    /// <param name="model">Model containing fields.</param>
    /// <param name="schema">Schema for type mapping and enum detection.</param>
    private static ExpressionSyntax BuildFieldsArray(ModelDefinition model, CharismaSchema schema)
    {
        var initializers = new List<ExpressionSyntax>();
        foreach (var field in model.Fields)
        {
            var (clrType, kind) = MapField(field, schema);
            var isPrimaryKey = model.PrimaryKey?.Fields.Contains(field.Name, StringComparer.Ordinal) ?? false;
            var isUnique = field is ScalarFieldDefinition scalarField && scalarField.IsUnique;
            var isUpdatedAt = field is ScalarFieldDefinition updatedAtField && updatedAtField.IsUpdatedAt;
            var defaultValueExpression = field is ScalarFieldDefinition scalarWithDefault
                ? BuildDefaultValueMetadata(scalarWithDefault.DefaultValue)
                : SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression);
            var creation = SyntaxFactory.ObjectCreationExpression(
                    SyntaxFactory.ParseTypeName("FieldMetadata"))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SeparatedList<ArgumentSyntax>(new SyntaxNodeOrToken[]
                        {
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(field.Name))),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(clrType))),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(field.IsList ? SyntaxKind.TrueLiteralExpression : SyntaxKind.FalseLiteralExpression)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(field.IsOptional ? SyntaxKind.TrueLiteralExpression : SyntaxKind.FalseLiteralExpression)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.MemberAccessExpression(
                                SyntaxKind.SimpleMemberAccessExpression,
                                SyntaxFactory.IdentifierName("FieldKind"),
                                SyntaxFactory.IdentifierName(kind))),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(isPrimaryKey ? SyntaxKind.TrueLiteralExpression : SyntaxKind.FalseLiteralExpression)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(isUnique ? SyntaxKind.TrueLiteralExpression : SyntaxKind.FalseLiteralExpression)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(isUpdatedAt ? SyntaxKind.TrueLiteralExpression : SyntaxKind.FalseLiteralExpression)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(defaultValueExpression)
                        })));
            initializers.Add(creation);
        }

        return BuildArrayExpression("FieldMetadata", initializers);
    }

    /// <summary>
    /// Builds ForeignKeyMetadata array for relation fields.
    /// </summary>
    /// <param name="model">Model owning relation fields.</param>
    /// <param name="schema">Schema used to infer principal keys when absent.</param>
    private static ExpressionSyntax BuildForeignKeysArray(ModelDefinition model, CharismaSchema schema)
    {
        var initializers = new List<ExpressionSyntax>();
        foreach (var field in model.Fields)
        {
            if (field is not RelationFieldDefinition relation || relation.RelationInfo is null)
            {
                continue;
            }

            var info = relation.RelationInfo;
            var principalFields = info.ForeignFields;

            // If the foreign fields are not specified, default to the primary key of the referenced model.
            if ((principalFields == null || principalFields.Count == 0)
                && schema.Models.TryGetValue(info.ForeignModel, out var foreignModel)
                && foreignModel.PrimaryKey is { } pk)
            {
                principalFields = pk.Fields;
            }

            principalFields ??= Array.Empty<string>();
            var fkCreation = SyntaxFactory.ObjectCreationExpression(
                    SyntaxFactory.ParseTypeName("ForeignKeyMetadata"))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SeparatedList<ArgumentSyntax>(new SyntaxNodeOrToken[]
                        {
                            SyntaxFactory.Argument(BuildStringArray(info.LocalFields)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(info.ForeignModel))),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(BuildStringArray(principalFields)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(info.RelationName is null
                                ? SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)
                                : SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(info.RelationName))),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(
                                SyntaxFactory.MemberAccessExpression(
                                    SyntaxKind.SimpleMemberAccessExpression,
                                    SyntaxFactory.IdentifierName("OnDeleteBehavior"),
                                    SyntaxFactory.IdentifierName(info.OnDelete.ToString())))
                        })));

            initializers.Add(fkCreation);
        }

        return BuildArrayExpression("ForeignKeyMetadata", initializers);
    }

    /// <summary>
    /// Builds IndexMetadata array for model indexes.
    /// </summary>
    /// <param name="indexes">Indexes to emit.</param>
    private static ExpressionSyntax BuildIndexesArray(IReadOnlyList<IndexDefinition> indexes)
    {
        var initializers = new List<ExpressionSyntax>();
        foreach (var index in indexes)
        {
            var creation = SyntaxFactory.ObjectCreationExpression(
                    SyntaxFactory.ParseTypeName("IndexMetadata"))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SeparatedList<ArgumentSyntax>(new SyntaxNodeOrToken[]
                        {
                            SyntaxFactory.Argument(BuildStringArray(index.Fields)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(index.IsUnique ? SyntaxKind.TrueLiteralExpression : SyntaxKind.FalseLiteralExpression)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(index.Name is null
                                ? SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)
                                : SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(index.Name)))
                        })));
            initializers.Add(creation);
        }

        return BuildArrayExpression("IndexMetadata", initializers);
    }

    /// <summary>
    /// Builds UniqueConstraintMetadata array for model unique constraints.
    /// </summary>
    /// <param name="uniques">Unique constraints to emit.</param>
    private static ExpressionSyntax BuildUniqueConstraintsArray(IReadOnlyList<UniqueConstraintDefinition> uniques)
    {
        var initializers = new List<ExpressionSyntax>();
        foreach (var uc in uniques)
        {
            var creation = SyntaxFactory.ObjectCreationExpression(
                    SyntaxFactory.ParseTypeName("UniqueConstraintMetadata"))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SeparatedList<ArgumentSyntax>(new SyntaxNodeOrToken[]
                        {
                            SyntaxFactory.Argument(BuildStringArray(uc.Fields)),
                            SyntaxFactory.Token(SyntaxKind.CommaToken),
                            SyntaxFactory.Argument(uc.Name is null
                                ? SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)
                                : SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(uc.Name)))
                        })));
            initializers.Add(creation);
        }

        return BuildArrayExpression("UniqueConstraintMetadata", initializers);
    }

    /// <summary>
    /// Builds PrimaryKeyMetadata for a model.
    /// </summary>
    /// <param name="pk">Primary key definition.</param>
    private static ExpressionSyntax BuildPrimaryKeyMetadata(PrimaryKeyDefinition pk)
    {
        return SyntaxFactory.ObjectCreationExpression(
                SyntaxFactory.ParseTypeName("PrimaryKeyMetadata"))
            .WithArgumentList(
                SyntaxFactory.ArgumentList(
                    SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.Argument(BuildStringArray(pk.Fields)))));
    }

    /// <summary>
    /// Builds a string array literal expression from values.
    /// </summary>
    private static ExpressionSyntax BuildStringArray(IEnumerable<string> values)
    {
        var elements = new List<ExpressionSyntax>();
        foreach (var value in values)
        {
            elements.Add(SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(value)));
        }

        return BuildArrayExpression("string", elements);
    }

    /// <summary>
    /// Builds DefaultValueMetadata or null when absent.
    /// </summary>
    /// <param name="defaultValue">Default value definition (optional).</param>
    private static ExpressionSyntax BuildDefaultValueMetadata(DefaultValueDefinition? defaultValue)
    {
        if (defaultValue is null)
        {
            return SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression);
        }

        return SyntaxFactory.ObjectCreationExpression(
                SyntaxFactory.ParseTypeName("DefaultValueMetadata"))
            .WithArgumentList(
                SyntaxFactory.ArgumentList(
                    SyntaxFactory.SeparatedList<ArgumentSyntax>(new SyntaxNodeOrToken[]
                    {
                        SyntaxFactory.Argument(
                            SyntaxFactory.MemberAccessExpression(
                                SyntaxKind.SimpleMemberAccessExpression,
                                SyntaxFactory.IdentifierName("DefaultValueKind"),
                                SyntaxFactory.IdentifierName(defaultValue.Kind.ToString()))),
                        SyntaxFactory.Token(SyntaxKind.CommaToken),
                        SyntaxFactory.Argument(defaultValue.Value is null
                            ? SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)
                            : SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(defaultValue.Value)))
                    })));
    }

    /// <summary>
    /// Builds an array creation expression with supplied element type and initializers.
    /// </summary>
    private static ExpressionSyntax BuildArrayExpression(string elementType, IReadOnlyList<ExpressionSyntax> elements)
    {
        return SyntaxFactory.ArrayCreationExpression(
                SyntaxFactory.ArrayType(SyntaxFactory.ParseTypeName(elementType))
                    .WithRankSpecifiers(
                        SyntaxFactory.SingletonList(
                            SyntaxFactory.ArrayRankSpecifier(
                                SyntaxFactory.SingletonSeparatedList<ExpressionSyntax>(
                                    SyntaxFactory.OmittedArraySizeExpression())))))
            .WithInitializer(
                SyntaxFactory.InitializerExpression(
                    SyntaxKind.ArrayInitializerExpression,
                    SyntaxFactory.SeparatedList(elements)));
    }

    /// <summary>
    /// Maps a schema field to CLR type and FieldKind for metadata output.
    /// </summary>
    private static (string ClrType, string Kind) MapField(FieldDefinition field, CharismaSchema schema)
    {
        switch (field)
        {
            case ScalarFieldDefinition scalar:
                return (MapScalarClrType(scalar.RawType, schema), "Scalar");
            case RelationFieldDefinition relation:
                return (relation.RawType, "Relation");
            default:
                throw new InvalidOperationException($"Unsupported field type '{field.GetType().Name}'.");
        }
    }

    /// <summary>
    /// Maps scalar schema types (or enums) to CLR type names for metadata.
    /// </summary>
    private static string MapScalarClrType(string rawType, CharismaSchema schema)
    {
        if (schema.Enums.ContainsKey(rawType))
        {
            return rawType;
        }

        return rawType switch
        {
            "String" => "string",
            "Int" => "int",
            "Boolean" => "bool",
            "Float" => "double",
            "Decimal" => "decimal",
            "DateTime" => "DateTime",
            "Json" => "Json",
            "Bytes" => "byte[]",
            "UUID" or "Id" => "Guid",
            _ => throw new InvalidOperationException($"Unsupported scalar type '{rawType}'.")
        };
    }
}
