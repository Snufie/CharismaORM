using Charisma.Schema;
using System.Text.Json.Serialization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Writers;

/// <summary>
/// Generates args, data inputs, relation inputs, and SortOrder/OrderBy types per model.
/// </summary>
internal sealed class ArgsWriter : IWriter
{
    private readonly string _rootNamespace;
    private static HashSet<string> _enumNames = new(StringComparer.Ordinal);

    public ArgsWriter(string rootNamespace)
    {
        _rootNamespace = rootNamespace ?? throw new ArgumentNullException(nameof(rootNamespace));
    }

    /// <summary>
    /// Builds all argument and helper types for every model plus SortOrder.
    /// </summary>
    /// <param name="schema">Schema source to emit types from.</param>
    /// <returns>Compilation units containing args, inputs, relation helpers, and aggregates.</returns>
    public IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        _enumNames = new HashSet<string>(schema.Enums.Keys, StringComparer.Ordinal);

        var units = new List<CompilationUnitSyntax>
        {
            BuildSortOrderUnit()
        };

        var models = new List<ModelDefinition>(schema.Models.Values);
        models.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));

        foreach (var model in models)
        {
            units.Add(BuildArgsUnit(schema, model));
        }

        return units.AsReadOnly();
    }

    /// <summary>
    /// Generates the shared SortOrder enum used by OrderBy inputs.
    /// </summary>
    private CompilationUnitSyntax BuildSortOrderUnit()
    {
        var enumDecl = SyntaxFactory.EnumDeclaration("SortOrder")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddMembers(
                SyntaxFactory.EnumMemberDeclaration("Asc"),
                SyntaxFactory.EnumMemberDeclaration("Desc"));

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Args"))
            .AddMembers(enumDecl);

        return SyntaxFactory.CompilationUnit()
            .AddMembers(@namespace)
            .NormalizeWhitespace();
    }

    /// <summary>
    /// Generates all argument, input, and aggregate types for a single model.
    /// </summary>
    /// <param name="schema">Schema reference for enum/field lookup.</param>
    /// <param name="model">Model to generate types for.</param>
    private CompilationUnitSyntax BuildArgsUnit(CharismaSchema schema, ModelDefinition model)
    {
        var members = new List<MemberDeclarationSyntax>();

        // Arg types
        members.Add(BuildFindUniqueArgs(model));
        members.Add(BuildFindFirstArgs(model));
        members.Add(BuildFindManyArgs(model));
        members.Add(BuildCountArgs(model));
        members.Add(BuildAggregateArgs(schema, model));
        members.Add(BuildGroupByArgs(schema, model));
        members.Add(BuildCreateArgs(model));
        members.Add(BuildCreateManyArgs(model));
        members.Add(BuildUpdateArgs(model));
        members.Add(BuildUpdateManyArgs(model));
        members.Add(BuildDeleteArgs(model));
        members.Add(BuildDeleteManyArgs(model));
        members.Add(BuildUpsertArgs(model));

        // Data inputs
        members.Add(BuildCreateInput(schema, model));
        members.Add(BuildUpdateInput(schema, model));
        members.Add(BuildUpsertInput(schema, model));

        // Relation helper inputs
        members.Add(BuildCreateOrConnectInput(model));
        members.Add(BuildUpsertNestedInput(model));
        members.Add(BuildUpdateWithWhereInput(model));
        members.Add(BuildUpdateManyWithWhereInput(model));
        members.Add(BuildCreateRelationInput(model));
        members.Add(BuildCreateManyRelationInput(model));
        members.Add(BuildUpdateRelationInput(model));
        members.Add(BuildUpdateManyRelationInput(model));

        // Aggregate outputs
        members.Add(BuildAggregateResult(schema, model));
        members.Add(BuildAggregateMin(schema, model));
        members.Add(BuildAggregateMax(schema, model));
        var aggregateAvg = BuildAggregateAvg(schema, model);
        if (aggregateAvg is not null)
        {
            members.Add(aggregateAvg);
        }
        var aggregateSum = BuildAggregateSum(schema, model);
        if (aggregateSum is not null)
        {
            members.Add(aggregateSum);
        }

        members.Add(BuildGroupByOutput(schema, model));

        // Aggregate selectors
        members.Add(BuildAggregateSelectors(model));
        members.Add(BuildAggregateMinInput(model));
        members.Add(BuildAggregateMaxInput(model));
        var aggregateAvgInput = BuildAggregateAvgInput(model);
        if (aggregateAvgInput is not null)
        {
            members.Add(aggregateAvgInput);
        }
        var aggregateSumInput = BuildAggregateSumInput(model);
        if (aggregateSumInput is not null)
        {
            members.Add(aggregateSumInput);
        }

        // OrderBy input
        members.Add(BuildOrderByInput(schema, model));

        var @namespace = SyntaxFactory.NamespaceDeclaration(
                SyntaxFactory.ParseName($"{_rootNamespace}.Args"))
            .AddMembers(members.ToArray());

        var usings = new List<UsingDirectiveSyntax>
        {
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Collections.Generic")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Text.Json")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("System.Text.Json.Serialization")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Filters")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Select")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Omit")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Include")),
            SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("Charisma.Runtime"))
        };

        if (schema.Enums.Count > 0)
        {
            usings.Insert(usings.Count - 1, SyntaxFactory.UsingDirective(SyntaxFactory.ParseName($"{_rootNamespace}.Enums")));
        }

        return SyntaxFactory.CompilationUnit()
            .AddUsings(usings.ToArray())
            .AddMembers(@namespace)
            .NormalizeWhitespace();
    }

    /// <summary>
    /// Creates args for findUnique including select/omit/include and unique where.
    /// </summary>
    private static ClassDeclarationSyntax BuildFindUniqueArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}WhereUniqueInput?", "Where", "Unique filter; required."),
        };
        return BuildClass($"{model.Name}FindUniqueArgs", props, $"Arguments for {model.Name}.findUnique. Provide Where and optionally Select, Omit, or Include (choose one).");
    }

    /// <summary>
    /// Creates args for findMany with ordering, pagination, and selection knobs.
    /// </summary>
    private static ClassDeclarationSyntax BuildFindManyArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"IReadOnlyList<{model.Name}OrderByInput>?", "OrderBy", "Order results; stable with multiple directives."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp("IReadOnlyList<string>?", "Distinct", "Distinct on scalar fields; incompatible with Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}WhereUniqueInput?", "Cursor", "Cursor for pagination (unique)."),
            BuildProp("int?", "Skip", "Rows to skip (offset); must be non-negative."),
            BuildProp("int?", "Take", "Maximum rows to return; negative reverses order before limiting."),
            BuildProp($"{model.Name}WhereInput?", "Where", "Filter using AND/OR/NOT and field operators."),
        };
        return BuildClass($"{model.Name}FindManyArgs", props, $"Arguments for {model.Name}.findMany with filtering, ordering, paging, and select/omit/include (mutually exclusive).");
    }

    /// <summary>
    /// Creates args for findFirst mirroring findMany defaults.
    /// </summary>
    private static ClassDeclarationSyntax BuildFindFirstArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"IReadOnlyList<{model.Name}OrderByInput>?", "OrderBy", "Order results; stable with multiple directives."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp("IReadOnlyList<string>?", "Distinct", "Distinct on scalar fields; incompatible with Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}WhereUniqueInput?", "Cursor", "Cursor for pagination (unique)."),
            BuildProp("int?", "Skip", "Rows to skip (offset); must be non-negative."),
            BuildProp("int?", "Take", "Maximum rows to consider; negative reverses order before limiting."),
            BuildProp($"{model.Name}WhereInput?", "Where", "Filter using AND/OR/NOT and field operators."),
        };
        return BuildClass($"{model.Name}FindFirstArgs", props, $"Arguments for {model.Name}.findFirst. Behaves like findMany with implicit take=1 unless overridden; Select/Omit/Include are mutually exclusive, and Distinct cannot be combined with Include.");
    }

    /// <summary>
    /// Creates args for counting with optional filters and distinct.
    /// </summary>
    private static ClassDeclarationSyntax BuildCountArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}WhereInput?", "Where", "Optional filter for counting."),
            BuildProp("IReadOnlyList<string>?", "Distinct", "Count distinct combinations of scalar fields."),
        };
        return BuildClass($"{model.Name}CountArgs", props, $"Arguments for counting {model.Name} records with optional filter and distinct fields.");
    }

    /// <summary>
    /// Creates args for aggregate queries including selectors and paging.
    /// </summary>
    private ClassDeclarationSyntax BuildAggregateArgs(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>
        {
            BuildProp($"{model.Name}WhereInput?", "Where", "Filter before aggregation."),
            BuildProp($"IReadOnlyList<{model.Name}OrderByInput>?", "OrderBy", "Apply ordering prior to take/skip."),
            BuildProp($"{model.Name}WhereUniqueInput?", "Cursor", "Cursor for pagination (unique)."),
            BuildProp("int?", "Skip", "Rows to skip (offset); must be non-negative."),
            BuildProp("int?", "Take", "Maximum rows to consider; must be positive when provided."),
            BuildProp("IReadOnlyList<string>?", "Distinct", "Distinct on scalar fields before aggregating."),
            BuildProp($"{model.Name}AggregateSelectors?", "Aggregate", "Aggregate selectors (count/min/max/avg/sum) to compute in one request.")
        };

        return BuildClass($"{model.Name}AggregateArgs", props, $"Aggregate arguments for {model.Name} including filters, paging, and aggregate selectors.");
    }

    /// <summary>
    /// Creates args for groupBy including HAVING, paging, and aggregates.
    /// </summary>
    private ClassDeclarationSyntax BuildGroupByArgs(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>
        {
            BuildProp($"{model.Name}WhereInput?", "Where", "Filter records before grouping."),
            BuildProp($"IReadOnlyList<{model.Name}OrderByInput>?", "OrderBy", "Order groups by grouped fields."),
            BuildProp("IReadOnlyList<string>?", "By", "Fields to group by; must reference scalar fields."),
            BuildProp($"{model.Name}WhereInput?", "Having", "Filter groups after aggregation (applied in HAVING)."),
            BuildProp("int?", "Take", "Limit the number of groups returned; must be positive when provided."),
            BuildProp("int?", "Skip", "Skip the first N groups; must be non-negative."),
            BuildProp("bool?", "_count", "Include COUNT(*) for each group."),
            BuildProp($"{model.Name}AggregateMinInput?", "_min", "Select scalar fields to compute MIN per group."),
            BuildProp($"{model.Name}AggregateMaxInput?", "_max", "Select scalar fields to compute MAX per group."),
        };

        if (HasNumericScalars(model))
        {
            props.Add(BuildProp($"{model.Name}AggregateAvgInput?", "_avg", "Select numeric fields to compute AVG per group."));
            props.Add(BuildProp($"{model.Name}AggregateSumInput?", "_sum", "Select numeric fields to compute SUM per group."));
        }

        return BuildClass($"{model.Name}GroupByArgs", props, $"Arguments for {model.Name}.groupBy including by-fields, filters, ordering, paging, and aggregate selections.");
    }

    /// <summary>
    /// Creates args for single create operations.
    /// </summary>
    private static ClassDeclarationSyntax BuildCreateArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}CreateInput", "Data", "Values to create; supports nested creates/connects."),
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
        };
        return BuildClass($"{model.Name}CreateArgs", props, $"Arguments for {model.Name}.create. Provide Data and optionally Select or Omit (include is unsupported).");
    }

    /// <summary>
    /// Creates args for batch create operations.
    /// </summary>
    private static ClassDeclarationSyntax BuildCreateManyArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"IReadOnlyList<{model.Name}CreateInput>", "Data", "Batch payload; must contain at least one item."),
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp("bool?", "SkipDuplicates", "If true, duplicate conflicts are skipped (ON CONFLICT DO NOTHING)."),
        };
        return BuildClass($"{model.Name}CreateManyArgs", props, $"Arguments for {model.Name}.createMany. Data cannot be empty; Select and Omit are exclusive and Include is unsupported.");
    }

    /// <summary>
    /// Creates args for single update operations.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpdateArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}UpdateInput", "Data", "Fields to update; supports nested writes as allowed."),
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}WhereUniqueInput", "Where", "Unique filter of the target row; required."),
        };
        return BuildClass($"{model.Name}UpdateArgs", props, $"Arguments for {model.Name}.update. Provide Where and Data; Select and Omit are exclusive and Include is unsupported.");
    }

    /// <summary>
    /// Creates args for multi-row updates.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpdateManyArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}UpdateInput", "Data", "Fields to update for matching rows."),
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}WhereInput?", "Where", "Filter of rows to update."),
            BuildProp("int?", "Limit", "Optional maximum rows to affect; requires deterministic ordering for consistent results."),
        };
        return BuildClass($"{model.Name}UpdateManyArgs", props, $"Arguments for {model.Name}.updateMany. Provide Data and optional Where; Select and Omit are exclusive and Include is unsupported.");
    }

    /// <summary>
    /// Creates args for single deletes.
    /// </summary>
    private static ClassDeclarationSyntax BuildDeleteArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}WhereUniqueInput", "Where", "Unique filter of the target row; required."),
        };
        return BuildClass($"{model.Name}DeleteArgs", props, $"Arguments for {model.Name}.delete. Provide Where; Select and Omit are exclusive and Include is unsupported.");
    }

    /// <summary>
    /// Creates args for multi-row deletes.
    /// </summary>
    private static ClassDeclarationSyntax BuildDeleteManyArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}WhereInput?", "Where", "Filter of rows to delete."),
            BuildProp("int?", "Limit", "Optional maximum rows to delete; apply a Where or Limit to avoid full-table deletes."),
        };
        return BuildClass($"{model.Name}DeleteManyArgs", props, $"Arguments for {model.Name}.deleteMany. Optional Where; Select and Omit are exclusive and Include is unsupported.");
    }

    /// <summary>
    /// Creates args for upsert operations combining create and update.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpsertArgs(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}CreateInput", "Create", "Create payload when no row matches Where."),
            BuildProp($"{model.Name}Include?", "Include", "Include related records; mutually exclusive with Select."),
            BuildProp($"{model.Name}Omit?", "Omit", "Exclude scalar fields; mutually exclusive with Select/Include."),
            BuildProp($"{model.Name}Select?", "Select", "Pick scalar fields; mutually exclusive with Include."),
            BuildProp($"{model.Name}UpdateInput", "Update", "Update payload when a row matches Where."),
            BuildProp($"{model.Name}WhereUniqueInput", "Where", "Unique filter; required."),
        };
        return BuildClass($"{model.Name}UpsertArgs", props, $"Arguments for {model.Name}.upsert. Provide Where, Update, and Create; Select and Omit are exclusive and Include is unsupported.");
    }

    /// <summary>
    /// Builds create input payload types, including nested relation inputs.
    /// </summary>
    private ClassDeclarationSyntax BuildCreateInput(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        var fields = SortFields(model);

        foreach (var field in fields)
        {
            if (field is ScalarFieldDefinition scalar)
            {
                var allowNull = scalar.IsOptional || ShouldAllowOmitOnCreate(scalar);
                var typeName = MapScalarType(schema, scalar.RawType, allowNull);
                var summary = $"Value for '{field.Name}' when creating {model.Name}.";
                props.Add(BuildProp(typeName, field.Name, summary));
            }
            else if (field is RelationFieldDefinition relation)
            {
                var typeName = relation.IsList
                    ? $"{relation.RawType}CreateManyRelationInput?"
                    : $"{relation.RawType}CreateRelationInput?";
                var summary = relation.IsList
                    ? $"Nested create/connect for many '{relation.Name}' relations."
                    : $"Nested create/connect for relation '{relation.Name}'.";
                props.Add(BuildProp(typeName, relation.Name, summary));
            }
        }

        return BuildClass($"{model.Name}CreateInput", props, $"Data payload for creating {model.Name} with optional nested relations.");
    }

    /// <summary>
    /// Builds update input payload types allowing optional fields and nested relations.
    /// </summary>
    private ClassDeclarationSyntax BuildUpdateInput(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        var fields = SortFields(model);

        foreach (var field in fields)
        {
            if (field is ScalarFieldDefinition scalar)
            {
                var typeName = MapScalarType(schema, scalar.RawType, nullable: true);
                var summary = $"Optional update for field '{field.Name}'.";
                props.Add(BuildProp(typeName, field.Name, summary));
            }
            else if (field is RelationFieldDefinition relation)
            {
                var typeName = relation.IsList
                    ? $"{relation.RawType}UpdateManyRelationInput?"
                    : $"{relation.RawType}UpdateRelationInput?";
                var summary = relation.IsList
                    ? $"Nested updates for many '{relation.Name}' relations."
                    : $"Nested updates for relation '{relation.Name}'.";
                props.Add(BuildProp(typeName, relation.Name, summary));
            }
        }

        return BuildClass($"{model.Name}UpdateInput", props, $"Data payload for updating {model.Name} including nested relations.");
    }

    /// <summary>
    /// Builds upsert input payload combining where, create, and update shapes.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpsertInput(CharismaSchema schema, ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}WhereUniqueInput", "Where", $"Unique selector determining whether to update or create {model.Name}."),
            BuildProp($"{model.Name}CreateInput", "Create", $"Create payload when no {model.Name} exists."),
            BuildProp($"{model.Name}UpdateInput", "Update", $"Update payload when a {model.Name} already exists.")
        };
        return BuildClass($"{model.Name}UpsertInput", props, $"Upsert payload combining where, create, and update shapes for {model.Name}.");
    }

    /// <summary>
    /// Builds OrderBy inputs for scalar fields and relations.
    /// </summary>
    private static ClassDeclarationSyntax BuildOrderByInput(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        var fields = SortFields(model);

        foreach (var field in fields)
        {
            if (field is ScalarFieldDefinition)
            {
                props.Add(BuildProp("SortOrder?", field.Name, $"Sort order for '{field.Name}'."));
            }
            else if (field is RelationFieldDefinition relation)
            {
                props.Add(BuildProp($"{relation.RawType}OrderByInput?", relation.Name, $"Ordering for related '{relation.Name}' records."));
            }
        }

        return BuildClass($"{model.Name}OrderByInput", props, $"Ordering input for {model.Name} fields and relations.");
    }

    /// <summary>
    /// Builds create-or-connect helper input for relations.
    /// </summary>
    private static ClassDeclarationSyntax BuildCreateOrConnectInput(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}CreateInput", "Create", $"Create a new {model.Name} when no existing record matches."),
            BuildProp($"{model.Name}WhereUniqueInput", "ConnectWhere", $"Unique selector to connect an existing {model.Name}.")
        };
        return BuildClass($"{model.Name}CreateOrConnectInput", props, $"Create or connect helper for {model.Name} relations.");
    }

    /// <summary>
    /// Builds nested upsert helper for relations.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpsertNestedInput(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}WhereUniqueInput", "Where", $"Unique selector for the related {model.Name} to update; create when not found."),
            BuildProp($"{model.Name}CreateInput", "Create", $"Create payload when related {model.Name} is missing."),
            BuildProp($"{model.Name}UpdateInput", "Update", $"Update payload when related {model.Name} exists.")
        };
        return BuildClass($"{model.Name}UpsertNestedInput", props, $"Nested upsert helper for {model.Name} relations including unique selector.");
    }

    /// <summary>
    /// Builds update-with-where helper for relations.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpdateWithWhereInput(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}UpdateInput", "Data", $"Fields to update on the related {model.Name}."),
            BuildProp($"{model.Name}WhereUniqueInput", "Where", $"Unique selector for the related {model.Name} to update.")
        };
        return BuildClass($"{model.Name}UpdateWithWhereInput", props, $"Update helper targeting a specific related {model.Name} by unique fields.");
    }

    /// <summary>
    /// Builds update-many-with-where helper for relations.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpdateManyWithWhereInput(ModelDefinition model)
    {
        var props = new[]
        {
            BuildProp($"{model.Name}UpdateInput", "Data", $"Fields to update on matching related {model.Name} records."),
            BuildProp($"{model.Name}WhereInput", "Where", $"Filter selecting related {model.Name} records to update.")
        };
        return BuildClass($"{model.Name}UpdateManyWithWhereInput", props, $"Update helper targeting many related {model.Name} records by filter.");
    }

    /// <summary>
    /// Builds nested create input for a single relation target.
    /// </summary>
    private static ClassDeclarationSyntax BuildCreateRelationInput(ModelDefinition target)
    {
        var props = new[]
        {
            BuildProp($"{target.Name}CreateInput?", "Create", $"Create and link a new {target.Name} record."),
            BuildProp($"{target.Name}CreateOrConnectInput?", "ConnectOrCreate", $"Connect existing {target.Name} or create if missing."),
            BuildProp($"{target.Name}WhereUniqueInput?", "Connect", $"Connect an existing {target.Name} via unique fields."),
        };
        return BuildClass($"{target.Name}CreateRelationInput", props, $"Nested create/connect options for a single {target.Name} relation.");
    }

    /// <summary>
    /// Builds nested create-many input for relation collections.
    /// </summary>
    private static ClassDeclarationSyntax BuildCreateManyRelationInput(ModelDefinition target)
    {
        var props = new[]
        {
            BuildProp($"IReadOnlyList<{target.Name}CreateInput>?", "Create", $"Create and link multiple new {target.Name} records."),
            BuildProp($"IReadOnlyList<{target.Name}CreateOrConnectInput>?", "ConnectOrCreate", $"Connect or create multiple {target.Name} records."),
            BuildProp($"IReadOnlyList<{target.Name}WhereUniqueInput>?", "Connect", $"Connect existing {target.Name} records by unique fields."),
        };
        return BuildClass($"{target.Name}CreateManyRelationInput", props, $"Nested create/connect options for many {target.Name} relations.");
    }

    /// <summary>
    /// Builds nested update input for a single relation.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpdateRelationInput(ModelDefinition target)
    {
        return SyntaxFactory.ClassDeclaration($"{target.Name}UpdateRelationInput")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.PartialKeyword))
            .WithLeadingTrivia(BuildDoc($"Nested update options for a single {target.Name} relation."))
            .AddMembers(
                BuildProp($"{target.Name}CreateInput?", "Create", $"Create and link a new {target.Name} record."),
                BuildProp($"{target.Name}CreateOrConnectInput?", "ConnectOrCreate", $"Connect an existing {target.Name} or create if missing."),
                BuildProp($"{target.Name}WhereUniqueInput?", "Connect", $"Connect an existing {target.Name} via unique fields."),
                BuildProp("bool?", "Delete", $"Delete the related {target.Name} record."),
                BuildProp("bool?", "Disconnect", $"Remove the relation without deleting the {target.Name} record."),
                BuildProp($"{target.Name}WhereUniqueInput?", "Replace", $"Replace the related record with another {target.Name} selected by unique fields."),
                // removed Set
                BuildProp("bool?", "DisconnectAll", $"Disconnect all related {target.Name} records."),
                BuildProp($"{target.Name}UpdateInput?", "Update", $"Update the related {target.Name} in place."),
                BuildProp($"{target.Name}UpsertNestedInput?", "Upsert", $"Update the related {target.Name} if it exists, otherwise create it.")
            );
    }

    /// <summary>
    /// Builds nested update input for relation collections.
    /// </summary>
    private static ClassDeclarationSyntax BuildUpdateManyRelationInput(ModelDefinition target)
    {
        return SyntaxFactory.ClassDeclaration($"{target.Name}UpdateManyRelationInput")
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword), SyntaxFactory.Token(SyntaxKind.PartialKeyword))
            .WithLeadingTrivia(BuildDoc($"Nested update options for many {target.Name} relations."))
            .AddMembers(
                BuildProp($"IReadOnlyList<{target.Name}CreateInput>?", "Create", $"Create and link multiple new {target.Name} records."),
                BuildProp($"IReadOnlyList<{target.Name}CreateOrConnectInput>?", "ConnectOrCreate", $"Connect or create multiple {target.Name} records."),
                BuildProp($"IReadOnlyList<{target.Name}WhereUniqueInput>?", "Connect", $"Connect existing {target.Name} records by unique fields."),
                BuildProp($"IReadOnlyList<{target.Name}WhereUniqueInput>?", "Delete", $"Delete related {target.Name} records by unique selector."),
                BuildProp($"IReadOnlyList<{target.Name}WhereInput>?", "DeleteMany", $"Delete related {target.Name} records matching the filter."),
                BuildProp($"IReadOnlyList<{target.Name}WhereUniqueInput>?", "Disconnect", $"Disconnect related {target.Name} records without deleting them."),
                BuildProp("bool?", "DisconnectAll", $"Disconnect all related {target.Name} records."),
                BuildProp($"IReadOnlyList<{target.Name}WhereUniqueInput>?", "Replace", $"Replace related {target.Name} records with the specified set."),
                // removed Set
                BuildProp($"IReadOnlyList<{target.Name}UpdateWithWhereInput>?", "Update", $"Update specific related {target.Name} records matched by unique fields."),
                BuildProp($"IReadOnlyList<{target.Name}UpdateManyWithWhereInput>?", "UpdateMany", $"Update related {target.Name} records matching the filter."),
                BuildProp($"IReadOnlyList<{target.Name}UpsertNestedInput>?", "Upsert", $"Upsert multiple related {target.Name} records.")
            );
    }

    /// <summary>
    /// Builds aggregate result envelope type.
    /// </summary>
    private ClassDeclarationSyntax BuildAggregateResult(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>
        {
            BuildProp("int?", "Count", "Row count when requested.")
        };

        props.Add(BuildProp($"{model.Name}AggregateMin?", "Min", "Minimum scalar values per requested field."));
        props.Add(BuildProp($"{model.Name}AggregateMax?", "Max", "Maximum scalar values per requested field."));

        if (HasNumericScalars(model))
        {
            props.Add(BuildProp($"{model.Name}AggregateAvg?", "Avg", "Average values for requested numeric fields."));
            props.Add(BuildProp($"{model.Name}AggregateSum?", "Sum", "Sum of requested numeric fields."));
        }

        return BuildClass($"{model.Name}AggregateResult", props, $"Aggregate result envelope for {model.Name} queries.");
    }

    /// <summary>
    /// Builds per-field minimum aggregate output type.
    /// </summary>
    private ClassDeclarationSyntax BuildAggregateMin(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is not ScalarFieldDefinition scalar)
            {
                continue;
            }

            var typeName = MapScalarType(schema, scalar.RawType, nullable: true);
            props.Add(BuildProp(typeName, scalar.Name, $"Minimum value observed for '{scalar.Name}'.", jsonIgnoreWhenNull: true));
        }

        return BuildClass($"{model.Name}AggregateMin", props, $"Minimum scalar values for {model.Name}.");
    }

    /// <summary>
    /// Builds per-field maximum aggregate output type.
    /// </summary>
    private ClassDeclarationSyntax BuildAggregateMax(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is not ScalarFieldDefinition scalar)
            {
                continue;
            }

            var typeName = MapScalarType(schema, scalar.RawType, nullable: true);
            props.Add(BuildProp(typeName, scalar.Name, $"Maximum value observed for '{scalar.Name}'.", jsonIgnoreWhenNull: true));
        }

        return BuildClass($"{model.Name}AggregateMax", props, $"Maximum scalar values for {model.Name}.");
    }

    /// <summary>
    /// Builds per-field average aggregate output type when numeric fields exist.
    /// </summary>
    private ClassDeclarationSyntax? BuildAggregateAvg(CharismaSchema schema, ModelDefinition model)
    {
        if (!HasNumericScalars(model))
        {
            return null;
        }

        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is not ScalarFieldDefinition scalar || !IsNumericScalar(scalar))
            {
                continue;
            }

            var typeName = MapAggregateNumericResultType(scalar.RawType, isAverage: true);
            props.Add(BuildProp(typeName, scalar.Name, $"Average value for '{scalar.Name}'.", jsonIgnoreWhenNull: true));
        }

        return BuildClass($"{model.Name}AggregateAvg", props, $"Average values for numeric fields of {model.Name}.");
    }

    /// <summary>
    /// Builds per-field sum aggregate output type when numeric fields exist.
    /// </summary>
    private ClassDeclarationSyntax? BuildAggregateSum(CharismaSchema schema, ModelDefinition model)
    {
        if (!HasNumericScalars(model))
        {
            return null;
        }

        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is not ScalarFieldDefinition scalar || !IsNumericScalar(scalar))
            {
                continue;
            }

            var typeName = MapAggregateNumericResultType(scalar.RawType, isAverage: false);
            props.Add(BuildProp(typeName, scalar.Name, $"Sum of '{scalar.Name}'.", jsonIgnoreWhenNull: true));
        }

        return BuildClass($"{model.Name}AggregateSum", props, $"Sum values for numeric fields of {model.Name}.");
    }

    /// <summary>
    /// Builds group-by output type containing grouped keys and aggregates.
    /// </summary>
    private ClassDeclarationSyntax BuildGroupByOutput(CharismaSchema schema, ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is not ScalarFieldDefinition scalar)
            {
                continue;
            }

            // GroupBy outputs should surface only requested keys; make them nullable so absent columns remain null.
            var typeName = MapScalarType(schema, scalar.RawType, nullable: true);
            props.Add(BuildProp(typeName, scalar.Name, $"Grouped key: {scalar.Name}.", jsonIgnoreWhenNull: true));
        }

        props.Add(BuildProp("int?", "Count", "COUNT(*) per group when requested.", jsonIgnoreWhenNull: true));
        props.Add(BuildProp($"{model.Name}AggregateMin?", "Min", "Minimum values per group when requested.", jsonIgnoreWhenNull: true));
        props.Add(BuildProp($"{model.Name}AggregateMax?", "Max", "Maximum values per group when requested.", jsonIgnoreWhenNull: true));

        if (HasNumericScalars(model))
        {
            props.Add(BuildProp($"{model.Name}AggregateAvg?", "Avg", "Average values per group when requested.", jsonIgnoreWhenNull: true));
            props.Add(BuildProp($"{model.Name}AggregateSum?", "Sum", "Sum values per group when requested.", jsonIgnoreWhenNull: true));
        }

        return BuildClass($"{model.Name}GroupByOutput", props, $"GroupBy result envelope for {model.Name} including grouped keys and aggregates.");
    }

    /// <summary>
    /// Builds aggregate selector input type.
    /// </summary>
    private ClassDeclarationSyntax BuildAggregateSelectors(ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>
        {
            BuildProp("bool?", "Count", "Include the total row count for the filtered set."),
            BuildProp($"{model.Name}AggregateMinInput?", "Min", "Select scalar fields to compute minimum values."),
            BuildProp($"{model.Name}AggregateMaxInput?", "Max", "Select scalar fields to compute maximum values."),
        };

        if (HasNumericScalars(model))
        {
            props.Add(BuildProp($"{model.Name}AggregateAvgInput?", "Avg", "Select numeric fields to compute averages."));
            props.Add(BuildProp($"{model.Name}AggregateSumInput?", "Sum", "Select numeric fields to compute sums."));
        }

        return BuildClass($"{model.Name}AggregateSelectors", props, $"Aggregate selectors for {model.Name}, spanning count, min, max, avg, and sum.");
    }

    /// <summary>
    /// Builds MIN aggregate selector input.
    /// </summary>
    private static ClassDeclarationSyntax BuildAggregateMinInput(ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is ScalarFieldDefinition scalar)
            {
                props.Add(BuildProp("bool?", scalar.Name, $"Request MIN for '{scalar.Name}'."));
            }
        }

        return BuildClass($"{model.Name}AggregateMinInput", props, $"Fields to include in MIN aggregate for {model.Name}.");
    }

    /// <summary>
    /// Builds MAX aggregate selector input.
    /// </summary>
    private static ClassDeclarationSyntax BuildAggregateMaxInput(ModelDefinition model)
    {
        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is ScalarFieldDefinition scalar)
            {
                props.Add(BuildProp("bool?", scalar.Name, $"Request MAX for '{scalar.Name}'."));
            }
        }

        return BuildClass($"{model.Name}AggregateMaxInput", props, $"Fields to include in MAX aggregate for {model.Name}.");
    }

    /// <summary>
    /// Builds AVG aggregate selector input when numeric fields exist.
    /// </summary>
    private static ClassDeclarationSyntax? BuildAggregateAvgInput(ModelDefinition model)
    {
        if (!HasNumericScalars(model))
        {
            return null;
        }

        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is ScalarFieldDefinition scalar && IsNumericScalar(scalar))
            {
                props.Add(BuildProp("bool?", scalar.Name, $"Request AVG for '{scalar.Name}'."));
            }
        }

        return BuildClass($"{model.Name}AggregateAvgInput", props, $"Numeric fields to include in AVG aggregate for {model.Name}.");
    }

    /// <summary>
    /// Builds SUM aggregate selector input when numeric fields exist.
    /// </summary>
    private static ClassDeclarationSyntax? BuildAggregateSumInput(ModelDefinition model)
    {
        if (!HasNumericScalars(model))
        {
            return null;
        }

        var props = new List<PropertyDeclarationSyntax>();
        foreach (var field in SortFields(model))
        {
            if (field is ScalarFieldDefinition scalar && IsNumericScalar(scalar))
            {
                props.Add(BuildProp("bool?", scalar.Name, $"Request SUM for '{scalar.Name}'."));
            }
        }

        return BuildClass($"{model.Name}AggregateSumInput", props, $"Numeric fields to include in SUM aggregate for {model.Name}.");
    }

    /// <summary>
    /// Checks if any numeric scalar fields exist on the model.
    /// </summary>
    private static bool HasNumericScalars(ModelDefinition model)
    {
        foreach (var field in SortFields(model))
        {
            if (field is ScalarFieldDefinition scalar && IsNumericScalar(scalar))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Determines if the scalar type supports numeric aggregation.
    /// </summary>
    private static bool IsNumericScalar(ScalarFieldDefinition scalar)
    {
        return scalar.RawType is "Int" or "Float" or "Decimal";
    }

    /// <summary>
    /// Maps raw numeric schema types to aggregate result CLR types.
    /// </summary>
    private static string MapAggregateNumericResultType(string rawType, bool isAverage)
    {
        return rawType switch
        {
            "Decimal" => "decimal?",
            "Int" => isAverage ? "double?" : "long?",
            "Float" => "double?",
            _ => "double?"
        };
    }

    /// <summary>
    /// Creates a public partial class with provided properties and optional summary docs.
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
    /// Builds a property with optional JSON ignore and doc trivia.
    /// </summary>
    private static PropertyDeclarationSyntax BuildProp(string typeName, string propName, string? summary = null, bool jsonIgnoreWhenNull = false)
    {
        var property = SyntaxFactory.PropertyDeclaration(SyntaxFactory.ParseTypeName(typeName), propName)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .AddAccessorListAccessors(
                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration).WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)));

        if (jsonIgnoreWhenNull)
        {
            var attr = SyntaxFactory.AttributeList(
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Attribute(SyntaxFactory.IdentifierName("JsonIgnore"))
                        .AddArgumentListArguments(
                            SyntaxFactory.AttributeArgument(
                                SyntaxFactory.NameEquals("Condition"),
                                null,
                                SyntaxFactory.MemberAccessExpression(
                                    SyntaxKind.SimpleMemberAccessExpression,
                                    SyntaxFactory.IdentifierName("JsonIgnoreCondition"),
                                    SyntaxFactory.IdentifierName("WhenWritingNull"))))));
            property = property.AddAttributeLists(attr);
        }

        if (!string.IsNullOrWhiteSpace(summary))
        {
            property = property.WithLeadingTrivia(BuildDoc(summary!));
        }

        if (ShouldInitializeToNullForgiving(typeName))
        {
            property = property.WithInitializer(
                    SyntaxFactory.EqualsValueClause(
                        SyntaxFactory.ParseExpression("null!")))
                .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken));
        }

        return property;
    }

    /// <summary>
    /// Creates minimal XML doc trivia for summaries.
    /// </summary>
    private static SyntaxTriviaList BuildDoc(string summary)
    {
        var trivia = SyntaxFactory.ParseLeadingTrivia($"/// <summary>{summary}</summary>\n");
        return trivia;
    }

    /// <summary>
    /// Determines whether a property should be null-forgiving initialized to silence warnings.
    /// </summary>
    private static bool ShouldInitializeToNullForgiving(string typeName)
    {
        if (typeName.EndsWith("?", StringComparison.Ordinal)) return false;

        var baseType = typeName.TrimEnd('?');
        if (_enumNames.Contains(baseType)) return false;

        // Be conservative: only null-forgive known reference-like types.
        // Unknown identifiers might be structs (e.g., Json, enums from external schemas).
        return baseType switch
        {
            "string" or "byte[]" => true,
            _ when baseType.StartsWith("IReadOnlyList<", StringComparison.Ordinal)
                || baseType.StartsWith("List<", StringComparison.Ordinal)
                || baseType.StartsWith("IEnumerable<", StringComparison.Ordinal)
                || baseType.StartsWith("Dictionary<", StringComparison.Ordinal)
                || baseType.StartsWith("IReadOnlyDictionary<", StringComparison.Ordinal) => true,
            _ => false
        };
    }

    /// <summary>
    /// Returns model fields sorted by name for stable output.
    /// </summary>
    private static IReadOnlyList<FieldDefinition> SortFields(ModelDefinition model)
    {
        var list = new List<FieldDefinition>(model.Fields);
        list.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
        return list;
    }

    /// <summary>
    /// Determines if a scalar field can be omitted on create due to default/updatedAt semantics.
    /// </summary>
    private static bool ShouldAllowOmitOnCreate(ScalarFieldDefinition field)
    {
        if (field.DefaultValue is not null)
        {
            return true;
        }

        if (field.IsUpdatedAt)
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Maps schema scalar types (and enums) to CLR type names with nullability.
    /// </summary>
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
}