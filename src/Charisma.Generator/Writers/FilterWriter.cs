using Charisma.Schema;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates scalar filters, where inputs, relation filters, and where-unique inputs.
/// </summary>
internal sealed class FilterWriter : IWriter
{
    private readonly string _rootNamespace;

    public FilterWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Emits scalar filter types, enum filters, and per-model where/unique/relation filters.
    /// </summary>
    /// <param name="schema">Schema source for models and enums.</param>
    /// <returns>Compilation units for scalar filters plus each model's filters.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        var units = new List<CompilationUnitSyntax>
        {
            BuildScalarFiltersUnit(schema)
        };

        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        foreach (var model in models)
        {
            units.Add(BuildModelFiltersUnit(schema, model));
        }

        return units.AsReadOnly();
    }

    /// <summary>
    /// Builds the shared scalar filter definitions and enum filters.
    /// </summary>
    private CompilationUnitSyntax BuildScalarFiltersUnit(CharismaSchema schema)
    {
        var members = new List<MemberDeclarationSyntax>
        {
            BuildStringComparisonModeEnum(),
            BuildStringFilter(),
            BuildNumberFilter("DateTime", "DateTime"),
            BuildNumberFilter("Decimal", "decimal"),
            BuildNumberFilter("Float", "double"),
            BuildNumberFilter("Int", "int"),
            BuildBoolFilter(),
            BuildGuidFilter(),
            BuildBytesFilter(),
            BuildJsonArrayFilter(),
            BuildJsonFilter(),
            BuildJsonPathFilter()
        };

        var enums = new List<EnumDefinition>(schema.Enums.Values);
        enums.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
        foreach (var en in enums)
        {
            members.Add(BuildEnumFilter(en.Name));
        }

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Filters"))
            .AddMembers(members.ToArray());

        return SyntaxFactory.CompilationUnit()
            .AddUsings(
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Collections.Generic")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Text.Json")),
                    SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Enums")),
                    SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Runtime")))
            .AddMembers(@namespace)
            .NormalizeWhitespace();
    }

    /// <summary>
    /// Builds string filter supporting comparisons and substring operations.
    /// </summary>
    private static ClassDeclarationSyntax BuildStringFilter()
    {
        var props = new[]
        {
            ("string?", "Contains"),
            ("string?", "EndsWith"),
            ("string?", "Equals"),
            ("string?", "Gt"),
            ("string?", "Gte"),
            ("IReadOnlyList<string>?", "In"),
            ("string?", "Lt"),
            ("string?", "Lte"),
            ("StringFilter?", "Not"),
            ("IReadOnlyList<string>?", "NotIn"),
            ("string?", "StartsWith"),
            ("StringComparisonMode?", "Mode")
        };
        return BuildClass("StringFilter", props, "String filter supporting equals/not/contains/startsWith/endsWith/lt/lte/gt/gte/in/notIn.");
    }

    /// <summary>
    /// Builds numeric/date filter with comparisons and set membership.
    /// </summary>
    /// <param name="name">Schema name of the type (e.g., Int, DateTime).</param>
    /// <param name="csharp">CLR type name.</param>
    private static ClassDeclarationSyntax BuildNumberFilter(string name, string csharp)
    {
        var props = new[]
        {
            ($"{csharp}?", "Equals"),
            ($"{csharp}?", "Gt"),
            ($"{csharp}?", "Gte"),
            ($"IReadOnlyList<{csharp}>?", "In"),
            ($"{csharp}?", "Lt"),
            ($"{csharp}?", "Lte"),
            ($"{name}Filter?", "Not"),
            ($"IReadOnlyList<{csharp}>?", "NotIn")
        };
        return BuildClass($"{name}Filter", props, $"Numeric filter for {name} with equals/not/in/notIn and comparison operators.");
    }

    /// <summary>
    /// Builds boolean filter.
    /// </summary>
    private static ClassDeclarationSyntax BuildBoolFilter()
    {
        var props = new[]
        {
            ("bool?", "Equals"),
            ("BoolFilter?", "Not")
        };
        return BuildClass("BoolFilter", props, "Boolean filter supporting equals and not.");
    }

    /// <summary>
    /// Builds GUID filter.
    /// </summary>
    private static ClassDeclarationSyntax BuildGuidFilter()
    {
        var props = new[]
        {
            ("Guid?", "Equals"),
            ("IReadOnlyList<Guid>?", "In"),
            ("GuidFilter?", "Not"),
            ("IReadOnlyList<Guid>?", "NotIn")
        };
        return BuildClass("GuidFilter", props, "GUID filter supporting equals/not/in/notIn.");
    }

    /// <summary>
    /// Builds bytes filter.
    /// </summary>
    private static ClassDeclarationSyntax BuildBytesFilter()
    {
        var props = new[]
        {
            ("byte[]?", "Equals"),
            ("IReadOnlyList<byte[]>?", "In"),
            ("BytesFilter?", "Not"),
            ("IReadOnlyList<byte[]>?", "NotIn")
        };
        return BuildClass("BytesFilter", props, "Binary filter supporting equals/not/in/notIn.");
    }

    /// <summary>
    /// Builds JSON filter combining equality, path, array, and string sub-filters.
    /// </summary>
    private static ClassDeclarationSyntax BuildJsonFilter()
    {
        var props = new[]
        {
            ("Json?", "Equals"),
            ("JsonFilter?", "Not"),
            ("JsonPathFilter?", "path"),
            ("JsonArrayFilter?", "array_contains"),
            ("StringFilter?", "stringFilter")
        };
        return BuildClass("JsonFilter", props, "JSON filter supporting equals/not, path queries, array helpers, and string comparisons.");
    }

    /// <summary>
    /// Builds JSON array filter helpers.
    /// </summary>
    private static ClassDeclarationSyntax BuildJsonArrayFilter()
    {
        var props = new[]
        {
            ("Json?", "Has"),
            ("IReadOnlyList<Json>?", "HasEvery"),
            ("IReadOnlyList<Json>?", "HasSome"),
            ("int?", "Length"),
            ("bool?", "IsEmpty")
        };
        return BuildClass("JsonArrayFilter", props, "Array-only JSON filter supporting contains/hasSome/hasEvery/length checks.");
    }

    /// <summary>
    /// Builds JSON path-scoped filter helpers.
    /// </summary>
    private static ClassDeclarationSyntax BuildJsonPathFilter()
    {
        var props = new[]
        {
            ("IReadOnlyList<string>?", "path"),
            ("Json?", "Equals"),
            ("JsonFilter?", "Not"),
            ("JsonArrayFilter?", "array_contains"),
            ("StringFilter?", "stringFilter")
        };

        return BuildClass("JsonPathFilter", props, "JSON filter scoped to a specific path (segments) within the JSON document.");
    }

    /// <summary>
    /// Builds string comparison mode enum controlling case sensitivity.
    /// </summary>
    private static EnumDeclarationSyntax BuildStringComparisonModeEnum()
    {
        return SyntaxFactory.EnumDeclaration("StringComparisonMode")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddMembers(
                SyntaxFactory.EnumMemberDeclaration("Default"),
                SyntaxFactory.EnumMemberDeclaration("Insensitive"),
                SyntaxFactory.EnumMemberDeclaration("Sensitive"))
            .WithLeadingTrivia(BuildDoc("Controls case-sensitivity for string filters."));
    }

    /// <summary>
    /// Builds enum filter for a specific enum type.
    /// </summary>
    private static ClassDeclarationSyntax BuildEnumFilter(string enumName)
    {
        var props = new[]
        {
            ($"{enumName}?", "Equals"),
            ($"IReadOnlyList<{enumName}>?", "In"),
            ($"{enumName}Filter?", "Not"),
            ($"IReadOnlyList<{enumName}>?", "NotIn")
        };
        return BuildClass($"{enumName}Filter", props, $"Enum filter for {enumName} supporting equals/not/in/notIn.");
    }

    /// <summary>
    /// Builds per-model where/unique/relation filter unit.
    /// </summary>
    private CompilationUnitSyntax BuildModelFiltersUnit(CharismaSchema schema, ModelDefinition model)
    {
        var members = new List<MemberDeclarationSyntax>
        {
            BuildWhereInput(schema, model),
            BuildWhereUniqueInput(schema, model),
            BuildRelationFilter(schema, model)
        };

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Filters"))
            .AddMembers(members.ToArray());

        return SyntaxFactory.CompilationUnit()
            .AddUsings(
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Collections.Generic")),
                SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Text.Json")),
                    SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Enums")),
                    SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Runtime")))
            .AddMembers(@namespace)
            .NormalizeWhitespace();
    }

    /// <summary>
    /// Builds logical where input combining conjunction/disjunction and field filters.
    /// </summary>
    private ClassDeclarationSyntax BuildWhereInput(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>
        {
            BuildProp($"IReadOnlyList<{model.Name}WhereInput>?", "AND", "A and B"),
            BuildProp($"IReadOnlyList<{model.Name}WhereInput>?", "NOT", "Not A"),
            BuildProp($"IReadOnlyList<{model.Name}WhereInput>?", "OR", "A or B OR A and B"),
            BuildProp($"IReadOnlyList<{model.Name}WhereInput>?", "XOR", "A or B NOT A and B")
        };

        var fields = SortFields(model);
        foreach (var field in fields)
        {
            if (field is ScalarFieldDefinition scalar)
            {
                var filterType = GetScalarFilterType(schema, scalar.RawType);
                props.Add(BuildProp($"{filterType}?", field.Name));
            }
            else if (field is RelationFieldDefinition relation)
            {
                props.Add(BuildProp($"{relation.RawType}RelationFilter?", relation.Name));
            }
        }

        return BuildClass($"{model.Name}WhereInput", props, $"Logical filter for {model.Name} with AND/OR/XOR/NOT and per-field filters.");
    }

    /// <summary>
    /// Builds unique filter input using single-field unique constraints/PKs.
    /// </summary>
    private ClassDeclarationSyntax BuildWhereUniqueInput(CharismaSchema schema, ModelDefinition model)
    {
        var uniqueNames = new HashSet<string>(StringComparer.Ordinal);

        if (model.PrimaryKey is { } pk && pk.Fields.Count == 1)
        {
            uniqueNames.Add(pk.Fields[0]);
        }

        foreach (var uc in model.UniqueConstraints)
        {
            if (uc.Fields.Count == 1)
            {
                uniqueNames.Add(uc.Fields[0]);
            }
        }

        var props = uniqueNames
            .OrderBy(n => n, StringComparer.Ordinal)
            .Select(name =>
            {
                var field = model.GetField(name) ?? throw new InvalidOperationException($"Field '{name}' not found on model '{model.Name}'.");
                var typeSyntax = MapScalarOrEnumType(schema, field.RawType, nullable: true);
                return BuildProp(typeSyntax, name);
            })
            .ToList();

        return BuildClass($"{model.Name}WhereUniqueInput", props, $"Unique filter for {model.Name} using single-field unique or primary key values.");
    }

    /// <summary>
    /// Builds relation filter (every/some/none/is/isNot) for a target model.
    /// </summary>
    private ClassDeclarationSyntax BuildRelationFilter(CharismaSchema schema, ModelDefinition targetModel)
    {
        var props = new[]
        {
            BuildProp($"{targetModel.Name}WhereInput?", "Every"),
            BuildProp($"{targetModel.Name}WhereInput?", "Is"),
            BuildProp($"{targetModel.Name}WhereInput?", "IsNot"),
            BuildProp($"{targetModel.Name}WhereInput?", "None"),
            BuildProp($"{targetModel.Name}WhereInput?", "Some"),
        };

        return BuildClass($"{targetModel.Name}RelationFilter", props, $"Relation filter for {targetModel.Name} supporting every/some/none/is/isNot.");
    }

    /// <summary>
    /// Helper overload to build a class from type/name tuples.
    /// </summary>
    private static ClassDeclarationSyntax BuildClass(string name, IEnumerable<(string Type, string Name)> props, string? summary = null)
    {
        return BuildClass(name, props.Select(p => BuildProp(p.Type, p.Name)), summary);
    }

    /// <summary>
    /// Helper to build a class with supplied properties and optional summary doc.
    /// </summary>
    private static ClassDeclarationSyntax BuildClass(string name, IEnumerable<PropertyDeclarationSyntax> props, string? summary = null)
    {
        var cls = SyntaxFactory.ClassDeclaration(name)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.PartialKeyword))
            .AddMembers(props.ToArray());

        if (!string.IsNullOrWhiteSpace(summary))
        {
            cls = cls.WithLeadingTrivia(BuildDoc(summary!));
        }

        return cls;
    }

    /// <summary>
    /// Builds a property with optional new modifier and documentation.
    /// </summary>
    private static PropertyDeclarationSyntax BuildProp(string typeName, string propName, string? propAdditionalDoc = null)
    {
        var modifiers = propName == "Equals"
            ? new[] { SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.NewKeyword) }
            : new[] { SyntaxFactory.Token(SyntaxKind.PublicKeyword) };

        var property = SyntaxFactory.PropertyDeclaration(SyntaxFactory.ParseTypeName(typeName), propName)
            .AddModifiers(modifiers)
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)));

        property = property.WithLeadingTrivia(BuildDoc($"Filter clause for '{propName}' {propAdditionalDoc ?? string.Empty}."));

        if (ShouldInitializeToNullForgiving(typeName))
        {
            property = property.WithInitializer(
                SyntaxFactory.EqualsValueClause(
                    SyntaxFactory.PostfixUnaryExpression(
                        SyntaxKind.SuppressNullableWarningExpression,
                        SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression))))
                .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken));
        }

        return property;
    }

    /// <summary>
    /// Builds minimal XML doc trivia.
    /// </summary>
    private static SyntaxTriviaList BuildDoc(string summary)
    {
        return SyntaxFactory.ParseLeadingTrivia($"/// <summary>{summary}</summary>\n");
    }

    /// <summary>
    /// Determines whether the property type should use null-forgiving initializer.
    /// </summary>
    private static bool ShouldInitializeToNullForgiving(string typeName)
    {
        if (typeName.EndsWith("?", StringComparison.Ordinal)) return false;

        return typeName switch
        {
            "int" or "bool" or "double" or "decimal" or "DateTime" or "Guid" => false,
            _ => true
        };
    }

    /// <summary>
    /// Returns model fields sorted by name for deterministic output.
    /// </summary>
    private static IReadOnlyList<FieldDefinition> SortFields(ModelDefinition model)
    {
        var list = new List<FieldDefinition>(model.Fields);
        list.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
        return list;
    }

    /// <summary>
    /// Resolves the filter type name for a scalar or enum field.
    /// </summary>
    private static string GetScalarFilterType(CharismaSchema schema, string rawType)
    {
        if (schema.Enums.ContainsKey(rawType))
        {
            return $"{rawType}Filter";
        }

        return rawType switch
        {
            "String" => "StringFilter",
            "Int" => "IntFilter",
            "Float" => "FloatFilter",
            "Decimal" => "DecimalFilter",
            "DateTime" => "DateTimeFilter",
            "Boolean" => "BoolFilter",
            "UUID" or "Id" => "GuidFilter",
            "Bytes" => "BytesFilter",
            "Json" => "JsonFilter",
            _ => throw new InvalidOperationException($"Unsupported scalar type '{rawType}'.")
        };
    }

    /// <summary>
    /// Maps scalar or enum schema type to CLR type with optional nullability.
    /// </summary>
    private static string MapScalarOrEnumType(CharismaSchema schema, string rawType, bool nullable)
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

        if (nullable && typeName != "byte[]" && typeName != "string")
        {
            typeName += "?";
        }

        return nullable && (typeName == "string" || typeName == "byte[]") ? $"{typeName}?" : typeName;
    }
}