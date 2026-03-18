using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis.Text;
using Charisma.Schema;

namespace Charisma.Parser
{
    /// <summary>
    /// Roslyn-backed parser + validator (Prisma-like) for Charisma DSL.
    /// Produces a fully typed CharismaSchema; collects all diagnostics and throws aggregate if any.
    /// Supports Option A relation syntax (flexible, including fk: / pk: keyed args and named relations).
    /// </summary>
    public sealed class RoslynSchemaParser : ISchemaParser
    {
        // --- regexes for structure
        private static readonly Regex ModelHeaderRegex = new(@"^\s*model\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\{\s*$", RegexOptions.Compiled);
        private static readonly Regex EnumHeaderRegex = new(@"^\s*enum\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\{\s*$", RegexOptions.Compiled);
        private static readonly Regex DatasourceHeaderRegex = new(@"^\s*datasource\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\{\s*$", RegexOptions.Compiled);
        private static readonly Regex GeneratorHeaderRegex = new(@"^\s*generator\s+(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*\{\s*$", RegexOptions.Compiled);
        private static readonly Regex FieldLineRegex = new(@"^\s*(?<name>[A-Za-z_][A-Za-z0-9_]*)\s+(?<type>[^\s@]+)\s*(?<rest>.*)$", RegexOptions.Compiled);
        // Attribute parsing is done by a custom scanner to allow nested parentheses (e.g., @default(now())) and dotted names (@db.VarChar(255)).
        private static readonly Regex ModelDirectiveRegex = new(@"^@@(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*(\((?<args>.*)\))?\s*$", RegexOptions.Compiled);
        private static readonly Regex EnumValueRegex = new(@"^\s*(?<name>[A-Za-z_][A-Za-z0-9_]*)\s*$", RegexOptions.Compiled);
        private static readonly Regex KeyValueRegex = new(@"^\s*(?<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?<value>.+?)\s*$", RegexOptions.Compiled);

        // Scalars (including UUID)
        private static readonly HashSet<string> ScalarTypes = new(StringComparer.Ordinal)
        {
            "String", "Int", "Boolean", "Float", "DateTime", "Json", "Bytes", "Decimal", "UUID", "Uuid", "Id", "BigInt"
        };

        /// <summary>
        /// Splits a string by commas while respecting nesting depth for () and [].
        /// </summary>
        /// <param name="input">Argument list text.</param>
        /// <returns>Top-level comma segments trimmed of whitespace.</returns>
        private static List<string> SplitTopLevelComma(string input)
        {
            var list = new List<string>();
            if (string.IsNullOrWhiteSpace(input)) return list;
            int depth = 0;
            int start = 0;
            for (int i = 0; i < input.Length; i++)
            {
                char c = input[i];
                if (c == '(' || c == '[') depth++;
                else if (c == ')' || c == ']') depth--;
                else if (c == ',' && depth == 0)
                {
                    list.Add(input[start..i].Trim());
                    start = i + 1;
                }
            }
            list.Add(input[start..].Trim());
            return list.Where(s => s.Length > 0).ToList();
        }

        private static string StripQuotes(string value)
        {
            var t = value.Trim();
            if (t.Length >= 2 && ((t.StartsWith("\"") && t.EndsWith("\"")) || (t.StartsWith("'") && t.EndsWith("'"))))
            {
                return t[1..^1];
            }

            return t;
        }

        /// <summary>
        /// Parse the schema text into a validated CharismaSchema.
        /// Collects all syntactic & semantic errors; throws CharismaSchemaAggregateException if any found.
        /// </summary>
        /// <param name="schemaText">Raw DSL contents of schema.charisma.</param>
        /// <returns>Validated in-memory schema; throws on diagnostic accumulation.</returns>
        public CharismaSchema Parse(string schemaText)
        {
            if (schemaText is null) throw new ArgumentNullException(nameof(schemaText));

            var sourceText = SourceText.From(schemaText);
            var lines = sourceText.Lines;

            var errors = new List<CharismaSchemaException>();

            // Pass 1 builders
            var modelBuilders = new Dictionary<string, ModelBuilder>(StringComparer.Ordinal);
            var enums = new Dictionary<string, EnumDefinition>(StringComparer.Ordinal);
            var datasources = new List<DatasourceDefinition>();
            var generators = new List<GeneratorDefinition>();

            // Parse lines (Pass 1)
            int i = 0;
            while (i < lines.Count)
            {
                var rawLine = lines[i].ToString();
                var trimmed = rawLine.Trim();

                if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//"))
                {
                    i++;
                    continue;
                }

                // datasource
                var dsMatch = DatasourceHeaderRegex.Match(rawLine);
                if (dsMatch.Success)
                {
                    var name = dsMatch.Groups["name"].Value;
                    if (datasources.Any(d => string.Equals(d.Name, name, StringComparison.Ordinal)))
                    {
                        errors.Add(new CharismaSchemaException($"Duplicate datasource '{name}'", lines[i].Span));
                        i = SkipBlock(lines, i);
                        continue;
                    }

                    var (ds, next) = ParseDatasourceBlock(lines, i, sourceText, name, errors);
                    if (ds is not null) datasources.Add(ds);
                    i = next;
                    continue;
                }

                // generator
                var genMatch = GeneratorHeaderRegex.Match(rawLine);
                if (genMatch.Success)
                {
                    var name = genMatch.Groups["name"].Value;
                    if (generators.Any(g => string.Equals(g.Name, name, StringComparison.Ordinal)))
                    {
                        errors.Add(new CharismaSchemaException($"Duplicate generator '{name}'", lines[i].Span));
                        i = SkipBlock(lines, i);
                        continue;
                    }

                    var (gen, next) = ParseGeneratorBlock(lines, i, sourceText, name, errors);
                    if (gen is not null) generators.Add(gen);
                    i = next;
                    continue;
                }

                // model
                var mm = ModelHeaderRegex.Match(rawLine);
                if (mm.Success)
                {
                    var modelName = mm.Groups["name"].Value;
                    if (modelBuilders.ContainsKey(modelName))
                    {
                        errors.Add(new CharismaSchemaException($"Duplicate model '{modelName}'", lines[i].Span));
                        i = SkipBlock(lines, i);
                        continue;
                    }
                    var (builder, next) = ParseModelBlock(lines, i, sourceText, modelName, errors);
                    modelBuilders[modelName] = builder;
                    i = next;
                    continue;
                }

                // enum
                var em = EnumHeaderRegex.Match(rawLine);
                if (em.Success)
                {
                    var enumName = em.Groups["name"].Value;
                    if (enums.ContainsKey(enumName))
                    {
                        errors.Add(new CharismaSchemaException($"Duplicate enum '{enumName}'", lines[i].Span));
                        i = SkipBlock(lines, i);
                        continue;
                    }
                    var (enumDef, next) = ParseEnumBlock(lines, i, sourceText, enumName, errors);
                    enums[enumName] = enumDef;
                    i = next;
                    continue;
                }

                // unknown top-level token
                errors.Add(new CharismaSchemaException($"Unexpected top-level token: '{trimmed}'", lines[i].Span));
                i++;
            }

            if (errors.Count > 0) throw new CharismaSchemaAggregateException(errors);

            // Prepare sets for semantic validation
            var modelNames = new HashSet<string>(modelBuilders.Keys, StringComparer.Ordinal);
            var enumNames = new HashSet<string>(enums.Keys, StringComparer.Ordinal);

            // Pass 2: validate builders and construct final ModelDefinition instances
            var finalModels = new Dictionary<string, ModelDefinition>(StringComparer.Ordinal);

            foreach (var kv in modelBuilders)
            {
                var builder = kv.Value;
                var modelErrorsStartCount = errors.Count;

                // field-level checks and scaffolding
                var fieldNameSet = new HashSet<string>(StringComparer.Ordinal);
                PrimaryKeyDefinition? pk = null;
                var uniques = new List<UniqueConstraintDefinition>();
                var indexes = new List<IndexDefinition>();

                // Validate fields and types
                foreach (var fb in builder.Fields)
                {
                    if (!fieldNameSet.Add(fb.Name))
                    {
                        errors.Add(new CharismaSchemaException($"Duplicate field '{fb.Name}' in model '{builder.Name}'", fb.Span));
                        continue;
                    }

                    var isRelation = modelNames.Contains(fb.RawType);

                    // Primary key, uniqueness, updatedAt, default validation all occur here to keep errors close to source spans.

                    // validate @id
                    if (fb.Attributes.Any(a => IsAttributeName(a, "id")))
                    {
                        if (isRelation)
                        {
                            errors.Add(new CharismaSchemaException($"@id cannot be applied to relation field '{fb.Name}' in model '{builder.Name}'", fb.Span));
                        }
                        else if (fb.IsOptional)
                        {
                            errors.Add(new CharismaSchemaException($"@id field '{fb.Name}' in model '{builder.Name}' cannot be optional", fb.Span));
                        }
                        else if (fb.IsList)
                        {
                            errors.Add(new CharismaSchemaException($"@id field '{fb.Name}' in model '{builder.Name}' cannot be a list", fb.Span));
                        }
                        else if (pk is not null)
                        {
                            errors.Add(new CharismaSchemaException($"Multiple primary keys declared on model '{builder.Name}' (field '{fb.Name}')", fb.Span));
                        }
                        else
                        {
                            pk = new PrimaryKeyDefinition([fb.Name]);
                            fb.IsId = true;
                        }
                    }

                    // validate @unique
                    if (fb.Attributes.Any(a => IsAttributeName(a, "unique")))
                    {
                        if (isRelation)
                        {
                            errors.Add(new CharismaSchemaException($"@unique cannot be applied to relation field '{fb.Name}' in model '{builder.Name}'", fb.Span));
                        }
                        else
                        {
                            uniques.Add(new UniqueConstraintDefinition([fb.Name]));
                            fb.IsUnique = true;
                        }
                    }

                    // validate @updatedAt
                    if (fb.Attributes.Any(a => IsAttributeName(a, "updatedAt")))
                    {
                        if (isRelation || fb.IsList)
                        {
                            errors.Add(new CharismaSchemaException($"@updatedAt cannot be applied to relation or list field '{fb.Name}' in model '{builder.Name}'", fb.Span));
                        }
                        else if (!string.Equals(fb.RawType, "DateTime", StringComparison.Ordinal))
                        {
                            errors.Add(new CharismaSchemaException($"@updatedAt field '{fb.Name}' in model '{builder.Name}' must be of type DateTime", fb.Span));
                        }
                        else
                        {
                            fb.IsUpdatedAt = true;
                        }
                    }

                    // validate @default
                    var defaultAttr = fb.Attributes.FirstOrDefault(a => IsAttributeName(a, "default"));
                    if (defaultAttr is not null)
                    {
                        if (isRelation || fb.IsList)
                        {
                            errors.Add(new CharismaSchemaException($"@default cannot be applied to relation or list field '{fb.Name}' in model '{builder.Name}'", fb.Span));
                        }
                        else
                        {
                            var parsedDefault = ParseDefaultAttribute(defaultAttr, fb, builder.Name, errors);
                            fb.DefaultValue = parsedDefault;
                        }
                    }

                    // Validate raw type: scalar | enum | model
                    if (ScalarTypes.Contains(fb.RawType))
                    {
                        // OK
                    }
                    else if (enumNames.Contains(fb.RawType))
                    {
                        // OK
                    }
                    else if (modelNames.Contains(fb.RawType))
                    {
                        // Relation — validated below
                    }
                    else
                    {
                        errors.Add(new CharismaSchemaException($"Unknown type '{fb.RawType}' on field '{fb.Name}' in model '{builder.Name}'", fb.Span));
                    }
                }

                // UUIDv7 already embeds creation time, so prohibit a parallel created_at marker field.
                var hasUuidV7Pk = builder.Fields.Any(f =>
                    f.IsId
                    && f.DefaultValue?.Kind == DefaultValueKind.UuidV7);
                if (hasUuidV7Pk)
                {
                    var createdMarker = builder.Fields.FirstOrDefault(f =>
                        string.Equals(f.Name, "created_at", StringComparison.OrdinalIgnoreCase)
                        || string.Equals(f.Name, "createdAt", StringComparison.OrdinalIgnoreCase));
                    if (createdMarker is not null)
                    {
                        errors.Add(new CharismaSchemaException(
                            $"Model '{builder.Name}' uses UUIDv7 primary keys; remove '{createdMarker.Name}' because creation timestamp is encoded in the UUID.",
                            createdMarker.Span));
                    }
                }

                // Parse model-level directives (@@id, @@unique, @@index)
                foreach (var raw in builder.ModelLevelDirectivesRaw)
                {
                    var m = ModelDirectiveRegex.Match(raw);
                    if (!m.Success)
                    {
                        errors.Add(new CharismaSchemaException($"Malformed model-level directive '{raw}' on model '{builder.Name}'"));
                        continue;
                    }
                    var dname = m.Groups["name"].Value;
                    var args = m.Groups["args"].Value?.Trim();

                    if (string.Equals(dname, "id", StringComparison.Ordinal))
                    {
                        var fields = ParseBracketedFieldList(args);
                        if (fields.Count == 0) errors.Add(new CharismaSchemaException($"@@id on model '{builder.Name}' requires fields"));
                        else
                        {
                            if (pk is not null)
                            {
                                errors.Add(new CharismaSchemaException($"Conflicting primary key declarations on model '{builder.Name}'"));
                            }
                            pk = new PrimaryKeyDefinition(fields);
                        }
                    }
                    else if (string.Equals(dname, "unique", StringComparison.Ordinal))
                    {
                        var fields = ParseBracketedFieldList(args);
                        if (fields.Count == 0) errors.Add(new CharismaSchemaException($"@@unique on model '{builder.Name}' requires fields"));
                        else uniques.Add(new UniqueConstraintDefinition(fields));
                    }
                    else if (string.Equals(dname, "index", StringComparison.Ordinal))
                    {
                        var fields = ParseBracketedFieldList(args);
                        if (fields.Count == 0) errors.Add(new CharismaSchemaException($"@@index on model '{builder.Name}' requires fields"));
                        else indexes.Add(new IndexDefinition(fields, false));
                    }
                    else
                    {
                        // ignore unknown directives here
                    }
                }

                // Enforce primary key presence
                if (pk is null)
                {
                    errors.Add(new CharismaSchemaException($"Model '{builder.Name}' does not declare a primary key (@id or @@id required)"));
                }

                // Now construct final fields with typed RelationInfo where appropriate
                var finalFields = new List<FieldDefinition>();

                foreach (var fb in builder.Fields)
                {
                    if (modelNames.Contains(fb.RawType))
                    {
                        // Relation field
                        var relationInfo = ParseAndValidateRelationArgs(fb, builder.Name, modelNames, enumNames, finalModels, modelsPlaceholder: modelBuilders, errors);
                        var rfield = new RelationFieldDefinition(fb.Name, fb.RawType, fb.IsList, fb.IsOptional, fb.Attributes, fb.RelationAttributes, relationInfo);
                        finalFields.Add(rfield);
                    }
                    else
                    {
                        // Scalar or Enum field
                        var sfield = new ScalarFieldDefinition(fb.Name, fb.RawType, fb.IsList, fb.IsOptional, fb.Attributes, fb.IsId, fb.IsUnique, fb.IsUpdatedAt, fb.DefaultValue);
                        finalFields.Add(sfield);
                    }
                }

                // Build final ModelDefinition with typed constraints
                var finalModel = new ModelDefinition(builder.Name, finalFields, builder.ModelLevelDirectivesRaw, pk, uniques, indexes);
                finalModels[builder.Name] = finalModel;

                // Continue; errors collected in list
            }

            // After pass 2, if errors, throw aggregated
            if (errors.Count > 0) throw new CharismaSchemaAggregateException(errors);

            // Build final CharismaSchema (with datasources/generators)
            var finalSchema = new CharismaSchema(finalModels, enums, datasources, generators);
            return finalSchema;
        }

        private (DatasourceDefinition? ds, int nextIndex) ParseDatasourceBlock(IReadOnlyList<TextLine> lines, int startIndex, SourceText sourceText, string name, List<CharismaSchemaException> errors)
        {
            string? provider = null;
            string? url = null;
            var options = new Dictionary<string, string>(StringComparer.Ordinal);

            int i = startIndex + 1;
            for (; i < lines.Count; i++)
            {
                var raw = lines[i].ToString();
                var trimmed = raw.Trim();

                if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//")) continue;
                if (trimmed == "}")
                {
                    i++;
                    break;
                }

                var kv = KeyValueRegex.Match(raw);
                if (kv.Success)
                {
                    var key = kv.Groups["key"].Value;
                    var value = kv.Groups["value"].Value.Trim();

                    if (string.Equals(key, "provider", StringComparison.Ordinal)) provider = StripQuotes(value);
                    else if (string.Equals(key, "url", StringComparison.Ordinal)) url = value;
                    else options[key] = StripQuotes(value);

                    continue;
                }

                errors.Add(new CharismaSchemaException($"Unexpected token in datasource '{name}': '{trimmed}'", lines[i].Span));
            }

            if (i >= lines.Count && (url is null || provider is null))
            {
                errors.Add(new CharismaSchemaException($"Unterminated datasource '{name}'", lines[startIndex].Span));
                return (null, i);
            }

            if (provider is null) errors.Add(new CharismaSchemaException($"Datasource '{name}' is missing provider", lines[startIndex].Span));
            if (url is null) errors.Add(new CharismaSchemaException($"Datasource '{name}' is missing url", lines[startIndex].Span));

            if (provider is null || url is null) return (null, i);

            var ds = new DatasourceDefinition(name, provider, url, options);
            return (ds, i);
        }

        private (GeneratorDefinition? gen, int nextIndex) ParseGeneratorBlock(IReadOnlyList<TextLine> lines, int startIndex, SourceText sourceText, string name, List<CharismaSchemaException> errors)
        {
            var config = new Dictionary<string, string>(StringComparer.Ordinal);
            int i = startIndex + 1;

            for (; i < lines.Count; i++)
            {
                var raw = lines[i].ToString();
                var trimmed = raw.Trim();

                if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//")) continue;
                if (trimmed == "}")
                {
                    i++;
                    break;
                }

                var kv = KeyValueRegex.Match(raw);
                if (kv.Success)
                {
                    var key = kv.Groups["key"].Value;
                    var value = kv.Groups["value"].Value.Trim();
                    config[key] = StripQuotes(value);
                    continue;
                }

                errors.Add(new CharismaSchemaException($"Unexpected token in generator '{name}': '{trimmed}'", lines[i].Span));
            }

            if (i >= lines.Count)
            {
                errors.Add(new CharismaSchemaException($"Unterminated generator '{name}'", lines[startIndex].Span));
                return (null, i);
            }

            var gen = new GeneratorDefinition(name, config);
            return (gen, i);
        }

        // ---------------------
        // Relation parsing & validation (Option A flexible syntax)
        // ---------------------
        // Returns a populated RelationInfo if possible, otherwise null (but validation errors are added to `errors`)
        /// <summary>
        /// Parses @relation(...) args into RelationInfo and validates fk/pk targets.
        /// </summary>
        /// <param name="fb">Field builder carrying relation attributes.</param>
        /// <param name="localModelName">Model owning the relation field.</param>
        /// <param name="modelNames">Set of known model names.</param>
        /// <param name="enumNames">Set of known enum names.</param>
        /// <param name="finalModels">Models validated so far (unused in current path but reserved).</param>
        /// <param name="modelsPlaceholder">Raw builders for cross-checking fields.</param>
        /// <param name="errors">Error collection.</param>
        /// <returns>RelationInfo or null when none/insufficient info.</returns>
        private RelationInfo? ParseAndValidateRelationArgs(
            FieldBuilder fb,
            string localModelName,
            HashSet<string> modelNames,
            HashSet<string> enumNames,
            Dictionary<string, ModelDefinition> finalModels,
            Dictionary<string, ModelBuilder> modelsPlaceholder,
            List<CharismaSchemaException> errors)
        {
            // relationAttributes list contains raw args strings captured from @relation(...) occurrences on the same line.
            // Under Option A we expect at most one @relation on a field. If multiple the parser already errors earlier.
            // Allowed forms:
            // 1) positional name only: @relation("Name")
            // 2) function-style: @relation(fk(Model.Field), pk(Model.Field))
            // 3) keyed-style: @relation(fk: (Model.Field), pk: [Model.Field], name: "RelName")
            // 4) combination: @relation("Name", fk: (...), pk: (...))
            var rawList = fb.RelationAttributes;
            if (rawList.Count == 0) return null;

            var raw = rawList[0] ?? string.Empty;
            if (string.IsNullOrWhiteSpace(raw)) return null;

            // We will parse top-level comma-separated tokens
            var tokens = SplitTopLevelComma(raw);
            var fkRefs = new List<(string model, string field)>();
            var pkRefs = new List<(string model, string field)>();
            string? explicitName = null;
            OnDeleteBehavior? onDelete = null;

            foreach (var tok in tokens)
            {
                var t = tok.Trim();
                if (t.Length == 0) continue;

                // 1) positional quoted name: "Name" or 'Name'
                if ((t.StartsWith("\"") && t.EndsWith("\"")) || (t.StartsWith("'") && t.EndsWith("'")))
                {
                    var name = t[1..^1];
                    explicitName = name;
                    continue;
                }

                // 2) keyed style: key: value
                var colonIndex = t.IndexOf(':');
                if (colonIndex > 0)
                {
                    var key = t[..colonIndex].Trim();
                    var value = t[(colonIndex + 1)..].Trim();

                    if (string.Equals(key, "name", StringComparison.Ordinal))
                    {
                        var name = value.Trim().Trim('"').Trim('\'');
                        explicitName = name;
                        continue;
                    }

                    if (string.Equals(key, "onDelete", StringComparison.Ordinal))
                    {
                        var behaviorToken = value.Trim().Trim('"').Trim('\'');
                        if (TryParseOnDeleteBehavior(behaviorToken, out var parsedBehavior))
                        {
                            onDelete = parsedBehavior;
                        }
                        else
                        {
                            errors.Add(new CharismaSchemaException($"Unsupported onDelete behavior '{behaviorToken}' on field '{fb.Name}'", fb.Span));
                        }
                        continue;
                    }

                    if (string.Equals(key, "fk", StringComparison.Ordinal) || string.Equals(key, "pk", StringComparison.Ordinal))
                    {
                        // value can be "(...)" or "[...]" or bare "Model.Field"
                        value = value.Trim();
                        if (value.StartsWith("(") && value.EndsWith(")"))
                        {
                            var inner = value[1..^1].Trim();
                            var parts = SplitTopLevelComma(inner);
                            foreach (var p in parts)
                            {
                                ParseRelationEndpointToken(p, fb, fkMode: string.Equals(key, "fk", StringComparison.Ordinal), fkRefs, pkRefs, modelNames, modelsPlaceholder, errors);
                            }
                        }
                        else if (value.StartsWith("[") && value.EndsWith("]"))
                        {
                            var inner = value[1..^1].Trim();
                            var parts = SplitTopLevelComma(inner);
                            foreach (var p in parts)
                            {
                                ParseRelationEndpointToken(p, fb, fkMode: string.Equals(key, "fk", StringComparison.Ordinal), fkRefs, pkRefs, modelNames, modelsPlaceholder, errors);
                            }
                        }
                        else
                        {
                            // single token
                            ParseRelationEndpointToken(value, fb, fkMode: string.Equals(key, "fk", StringComparison.Ordinal), fkRefs, pkRefs, modelNames, modelsPlaceholder, errors);
                        }
                        continue;
                    }

                    // unknown key — try to handle name alias "name =" variants, otherwise ignore
                    if (string.Equals(key, "name", StringComparison.OrdinalIgnoreCase))
                    {
                        var name = value.Trim().Trim('"').Trim('\'');
                        explicitName = name;
                        continue;
                    }

                    // unknown keyed token — ignore for now
                    continue;
                }

                // 3) function style like fk(Model.Field) or pk(Model.Field)
                if (t.StartsWith("fk(", StringComparison.Ordinal) && t.EndsWith(")"))
                {
                    var inner = t[3..^1].Trim();
                    var parts = SplitTopLevelComma(inner);
                    foreach (var p in parts) ParseRelationEndpointToken(p, fb, fkMode: true, fkRefs, pkRefs, modelNames, modelsPlaceholder, errors);
                    continue;
                }
                if (t.StartsWith("pk(", StringComparison.Ordinal) && t.EndsWith(")"))
                {
                    var inner = t[3..^1].Trim();
                    var parts = SplitTopLevelComma(inner);
                    foreach (var p in parts) ParseRelationEndpointToken(p, fb, fkMode: false, fkRefs, pkRefs, modelNames, modelsPlaceholder, errors);
                    continue;
                }

                // 4) bare name (unquoted) — treat as explicit relation name
                explicitName ??= t.Trim().Trim('"').Trim('\'');
            }

            // Validate multiplicity: we expect at most one FK and at most one PK (single-field relations only).
            if (fkRefs.Count > 1)
            {
                errors.Add(new CharismaSchemaException($"Multiple fk(...) entries are not supported on field '{fb.Name}' in model '{fb.Name}'", fb.Span));
            }
            if (pkRefs.Count > 1)
            {
                errors.Add(new CharismaSchemaException($"Multiple pk(...) entries are not supported on field '{fb.Name}' in model '{fb.Name}'", fb.Span));
            }

            // Now validate collected fkRefs and pkRefs:
            foreach (var (mod, fld) in fkRefs.Concat(pkRefs))
            {
                if (!modelNames.Contains(mod))
                {
                    errors.Add(new CharismaSchemaException($"Relation on field '{fb.Name}' in model '{localModelName}' refers to unknown model '{mod}'", fb.Span));
                }
                else
                {
                    // check referenced model contains field
                    // Note: modelsPlaceholder has builders; builders contain FieldBuilder entries
                    if (modelsPlaceholder.TryGetValue(mod, out var mb))
                    {
                        if (!mb.Fields.Any(f => f.Name == fld))
                        {
                            errors.Add(new CharismaSchemaException($"Relation on field '{fb.Name}' in model '{localModelName}' refers to unknown field '{fld}' on model '{mod}'", fb.Span));
                        }
                    }
                }
            }

            // Build RelationInfo if we have enough info. It is acceptable for relation to omit fk/pk (inference later),
            // but we still create RelationInfo if at least pk/fk lists or explicit name present.
            var localFieldNames = fkRefs.Where(r => string.Equals(r.model, localModelName, StringComparison.Ordinal)).Select(r => r.field).ToList();
            var foreignFields = pkRefs.Select(r => r.field).ToList();

            bool anyParsed = fkRefs.Count > 0 || pkRefs.Count > 0 || explicitName is not null;
            if (!anyParsed) return null;

            var relationInfo = new RelationInfo(
                foreignModel: fb.RawType,
                localFields: localFieldNames,
                foreignFields: foreignFields,
                isCollection: fb.IsList,
                relationName: explicitName,
                onDelete: onDelete ?? OnDeleteBehavior.SetNull
            );

            return relationInfo;
        }

        /// <summary>
        /// Parses onDelete behavior token into enum value.
        /// </summary>
        private static bool TryParseOnDeleteBehavior(string token, out OnDeleteBehavior behavior)
        {
            switch (token.ToLowerInvariant())
            {
                case "cascade":
                    behavior = OnDeleteBehavior.Cascade;
                    return true;
                case "setnull":
                    behavior = OnDeleteBehavior.SetNull;
                    return true;
                case "restrict":
                    behavior = OnDeleteBehavior.Restrict;
                    return true;
                default:
                    behavior = OnDeleteBehavior.SetNull;
                    return false;
            }
        }

        private static void ParseRelationEndpointToken(
            string token,
            FieldBuilder fb,
            bool fkMode,
            List<(string model, string field)> fkRefs,
            List<(string model, string field)> pkRefs,
            HashSet<string> modelNames,
            Dictionary<string, ModelBuilder> modelsPlaceholder,
            List<CharismaSchemaException> errors)
        {
            if (token.Trim().Length == 0) return;

            // Clean token: remove wrapping parentheses if present (guard against short/empty tokens)
            token = token.Trim(' ', '\t', '(', ')');

            // Reject internal whitespace (C#-property-like syntax required)
            if (token.Contains(' ') || token.Contains('\t'))
            {
                errors.Add(new CharismaSchemaException(
                        $"Relation endpoint '{token}' on field '{fb.Name}' contains invalid whitespace",
                        fb.Span));
                return;
            }


            // Accept formats:
            // Model.Field
            // Maybe quoted or whitespacey: Model . Field
            var dot = token.IndexOf('.');
            if (dot <= 0 || dot == token.Length - 1)
            {
                errors.Add(new CharismaSchemaException($"Invalid relation endpoint token '{token}' on field '{fb.Name}'", fb.Span));
                return;
            }
            var mod = token[..dot].Trim();
            var fld = token[(dot + 1)..].Trim();

            if (fkMode) fkRefs.Add((mod, fld));
            else pkRefs.Add((mod, fld));
        }

        // ---------------------
        // Helpers for parsing blocks
        // ---------------------

        private static int SkipBlock(TextLineCollection lines, int startIndex)
        {
            int depth = 0;
            for (int j = startIndex; j < lines.Count; j++)
            {
                var text = lines[j].ToString();
                if (text.Contains('{')) depth++;
                if (text.Contains('}'))
                {
                    depth--;
                    if (depth <= 0) return j + 1;
                }
            }
            return lines.Count;
        }

        /// <summary>
        /// Parses a model block starting at the model header line.
        /// </summary>
        /// <param name="lines">Source lines.</param>
        /// <param name="startIndex">Index of the model header.</param>
        /// <param name="sourceText">Full source text (for spans).</param>
        /// <param name="modelName">Model name being parsed.</param>
        /// <param name="errors">Collection to append diagnostics.</param>
        /// <returns>Model builder and next line index after the block.</returns>
        private (ModelBuilder builder, int nextIndex) ParseModelBlock(TextLineCollection lines, int startIndex, SourceText sourceText, string modelName, List<CharismaSchemaException> errors)
        {
            var fields = new List<FieldBuilder>();
            var modelLevelDirectives = new List<string>();

            bool started = false;

            for (int i = startIndex; i < lines.Count; i++)
            {
                var raw = lines[i].ToString();
                var trimmed = raw.Trim();

                if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//")) continue;

                if (!started)
                {
                    if (trimmed.EndsWith('{')) { started = true; continue; }
                    errors.Add(new CharismaSchemaException($"Malformed model declaration for '{modelName}'", lines[i].Span));
                    return (new ModelBuilder(modelName, fields, modelLevelDirectives), i + 1);
                }

                // Only a standalone closing brace should terminate the block. Field literals may contain braces.
                if (trimmed == "}")
                {
                    return (new ModelBuilder(modelName, fields, modelLevelDirectives), i + 1);
                }

                var md = ModelDirectiveRegex.Match(trimmed);
                if (md.Success)
                {
                    modelLevelDirectives.Add(trimmed);
                    continue;
                }

                var fm = FieldLineRegex.Match(raw);
                if (fm.Success)
                {
                    var fname = fm.Groups["name"].Value;
                    var typeToken = fm.Groups["type"].Value;
                    var rest = fm.Groups["rest"].Value.Trim();

                    bool isList = false;
                    bool isOptional = false;
                    string rawType = typeToken;

                    // handle optional marker after list Type[]?
                    if (rawType.EndsWith('?'))
                    {
                        isOptional = true;
                        rawType = rawType[..^1];
                    }
                    if (rawType.EndsWith("[]"))
                    {
                        isList = true;
                        rawType = rawType[..^2];
                    }

                    var (attributes, relationAttributes) = ParseAttributes(rest);

                    var fbuilder = new FieldBuilder(fname, rawType, isList, isOptional, attributes, relationAttributes, lines[i].Span);
                    fields.Add(fbuilder);
                    continue;
                }

                errors.Add(new CharismaSchemaException($"Unexpected token in model '{modelName}': '{trimmed}'", lines[i].Span));
            }

            errors.Add(new CharismaSchemaException($"Unterminated model block for '{modelName}'", default));
            return (new ModelBuilder(modelName, fields, modelLevelDirectives), lines.Count);
        }

        /// <summary>
        /// Parses an enum block and returns the definition plus the index after the block.
        /// </summary>
        private (EnumDefinition enumeration, int nextIndex) ParseEnumBlock(TextLineCollection lines, int startIndex, SourceText sourceText, string enumName, List<CharismaSchemaException> errors)
        {
            var values = new List<string>();

            bool started = false;

            for (int i = startIndex; i < lines.Count; i++)
            {
                var raw = lines[i].ToString();
                var trimmed = raw.Trim();

                if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith("//")) continue;

                if (!started)
                {
                    if (trimmed.EndsWith('{')) { started = true; continue; }
                    errors.Add(new CharismaSchemaException($"Malformed enum declaration for '{enumName}'", lines[i].Span));
                    return (new EnumDefinition(enumName, values), i + 1);
                }

                // Only a standalone closing brace should terminate the block.
                if (trimmed == "}")
                {
                    return (new EnumDefinition(enumName, values), i + 1);
                }

                var m = EnumValueRegex.Match(trimmed);
                if (m.Success)
                {
                    values.Add(m.Groups["name"].Value);
                    continue;
                }

                errors.Add(new CharismaSchemaException($"Unexpected enum token '{trimmed}' in enum '{enumName}'", lines[i].Span));
            }

            errors.Add(new CharismaSchemaException($"Unterminated enum block for '{enumName}'", default));
            return (new EnumDefinition(enumName, values), lines.Count);
        }

        /// <summary>
        /// Checks whether a raw @attribute token matches the provided attribute name.
        /// </summary>
        private static bool IsAttributeName(string rawAttrToken, string name)
        {
            if (string.IsNullOrEmpty(rawAttrToken)) return false;
            if (!rawAttrToken.StartsWith('@')) return false;
            var after = rawAttrToken[1..];
            var idx = after.IndexOf('(');
            var key = idx >= 0 ? after[..idx] : after;
            return string.Equals(key, name, StringComparison.Ordinal);
        }

        /// <summary>
        /// Extracts the argument substring from an @attribute(token) string.
        /// </summary>
        private static string? ExtractAttributeArgs(string rawAttrToken)
        {
            var start = rawAttrToken.IndexOf('(');
            var end = rawAttrToken.LastIndexOf(')');
            if (start < 0 || end <= start) return null;
            return rawAttrToken.Substring(start + 1, end - start - 1);
        }

        /// <summary>
        /// Parses @default(...) values and validates compatibility with the field.
        /// </summary>
        private static DefaultValueDefinition? ParseDefaultAttribute(string rawAttrToken, FieldBuilder fb, string modelName, List<CharismaSchemaException> errors)
        {
            var args = ExtractAttributeArgs(rawAttrToken);
            if (string.IsNullOrWhiteSpace(args))
            {
                errors.Add(new CharismaSchemaException($"@default on field '{fb.Name}' in model '{modelName}' requires a value", fb.Span));
                return null;
            }

            var token = args.Trim();
            var lowered = token.ToLowerInvariant();

            if (lowered.StartsWith("dbgenerated", StringComparison.Ordinal))
            {
                errors.Add(new CharismaSchemaException($"dbgenerated() defaults are not supported on field '{fb.Name}' in model '{modelName}'", fb.Span));
                return null;
            }

            if (lowered.StartsWith("autoincrement", StringComparison.Ordinal))
            {
                if (!string.Equals(fb.RawType, "Int", StringComparison.Ordinal))
                {
                    errors.Add(new CharismaSchemaException($"autoincrement() default on field '{fb.Name}' in model '{modelName}' requires Int type", fb.Span));
                    return null;
                }
                if (fb.IsOptional)
                {
                    errors.Add(new CharismaSchemaException($"autoincrement() default on field '{fb.Name}' in model '{modelName}' cannot be optional", fb.Span));
                    return null;
                }

                return new DefaultValueDefinition(DefaultValueKind.Autoincrement);
            }

            if (lowered.StartsWith("uuid", StringComparison.Ordinal))
            {
                if (!string.Equals(fb.RawType, "UUID", StringComparison.Ordinal)
                    && !string.Equals(fb.RawType, "Id", StringComparison.Ordinal))
                {
                    errors.Add(new CharismaSchemaException($"uuid() default on field '{fb.Name}' in model '{modelName}' requires UUID or Id type", fb.Span));
                    return null;
                }

                var isV7 = lowered.StartsWith("uuidv7(", StringComparison.Ordinal)
                    || lowered.Contains("uuid(7)", StringComparison.Ordinal);
                return new DefaultValueDefinition(isV7 ? DefaultValueKind.UuidV7 : DefaultValueKind.UuidV4);
            }

            if (string.Equals(lowered, "now()", StringComparison.Ordinal))
            {
                if (!string.Equals(fb.RawType, "DateTime", StringComparison.Ordinal))
                {
                    errors.Add(new CharismaSchemaException($"now() default on field '{fb.Name}' in model '{modelName}' requires DateTime type", fb.Span));
                    return null;
                }

                return new DefaultValueDefinition(DefaultValueKind.Now);
            }

            // Static or JSON values
            if (token.StartsWith('"') && token.EndsWith('"'))
            {
                var inner = token[1..^1];
                var kind = (inner.StartsWith("{", StringComparison.Ordinal) || inner.StartsWith("[", StringComparison.Ordinal))
                    ? DefaultValueKind.Json
                    : DefaultValueKind.Static;
                if (kind == DefaultValueKind.Json && !string.Equals(fb.RawType, "Json", StringComparison.Ordinal))
                {
                    errors.Add(new CharismaSchemaException($"JSON default on field '{fb.Name}' in model '{modelName}' requires Json type", fb.Span));
                    return null;
                }
                return new DefaultValueDefinition(kind, inner);
            }

            return new DefaultValueDefinition(DefaultValueKind.Static, token);
        }

        /// <summary>
        /// Parses a comma-separated or bracketed field list for model-level directives.
        /// </summary>
        private static IReadOnlyList<string> ParseBracketedFieldList(string? args)
        {
            if (string.IsNullOrWhiteSpace(args)) return Array.Empty<string>();
            var s = args.Trim();
            var start = s.IndexOf('(');
            var end = s.LastIndexOf(')');
            if (start >= 0 && end > start)
            {
                var inner = s.Substring(start + 1, end - start - 1);
                return SplitTopLevelComma(inner).Select(x => x.Trim()).Where(x => x.Length > 0).ToList();
            }
            return SplitTopLevelComma(s);
        }

        /// <summary>
        /// Parses attribute tokens from the tail of a field line, supporting nested parentheses and dotted names.
        /// Splits into general attributes and @relation attributes (args preserved raw).
        /// </summary>
        private static (List<string> Attributes, List<string> RelationAttributes) ParseAttributes(string rest)
        {
            var attrs = new List<string>();
            var relAttrs = new List<string>();

            int i = 0;
            while (i < rest.Length)
            {
                // Find the next '@'
                while (i < rest.Length && rest[i] != '@') i++;
                if (i >= rest.Length) break;

                int start = i;
                int depth = 0;
                i++; // move past '@'

                while (i < rest.Length)
                {
                    char c = rest[i];
                    if (c == '(') depth++;
                    else if (c == ')' && depth > 0) depth--;

                    // Attribute token ends when depth is zero and we hit a whitespace followed by non-')', or at next '@'
                    if (depth == 0 && c == '@')
                    {
                        break;
                    }

                    if (depth == 0 && char.IsWhiteSpace(c))
                    {
                        // Skip trailing spaces
                        int lookahead = i;
                        while (lookahead < rest.Length && char.IsWhiteSpace(rest[lookahead])) lookahead++;
                        if (lookahead >= rest.Length || rest[lookahead] == '@')
                        {
                            i = lookahead;
                            break;
                        }
                    }

                    i++;
                }

                var token = rest[start..i].Trim();
                if (token.Length == 0) continue;

                var name = ExtractAttributeName(token);
                if (string.Equals(name, "relation", StringComparison.Ordinal))
                {
                    var args = ExtractAttributeArgs(token) ?? string.Empty;
                    relAttrs.Add(args.Trim());
                }
                else
                {
                    attrs.Add(token);
                }
            }

            return (attrs, relAttrs);
        }

        private static string ExtractAttributeName(string rawAttrToken)
        {
            if (string.IsNullOrEmpty(rawAttrToken)) return string.Empty;
            if (rawAttrToken[0] == '@') rawAttrToken = rawAttrToken[1..];
            var idx = rawAttrToken.IndexOf('(');
            var end = idx >= 0 ? idx : rawAttrToken.Length;
            return rawAttrToken[..end];
        }

        // ---------------------
        // Internal builder types
        // ---------------------
        private sealed class ModelBuilder
        {
            public string Name { get; }
            public List<FieldBuilder> Fields { get; }
            public List<string> ModelLevelDirectivesRaw { get; }

            /// <summary>
            /// Captures intermediate parsed model state prior to validation.
            /// </summary>
            public ModelBuilder(string name, List<FieldBuilder> fields, List<string> modelLevelDirectivesRaw)
            {
                Name = name;
                Fields = fields;
                ModelLevelDirectivesRaw = modelLevelDirectivesRaw;
            }
        }

        private sealed class FieldBuilder
        {
            public string Name { get; }
            public string RawType { get; }
            public bool IsList { get; }
            public bool IsOptional { get; }
            public List<string> Attributes { get; }
            public List<string> RelationAttributes { get; }
            public TextSpan Span { get; }
            public bool IsId { get; set; }
            public bool IsUnique { get; set; }
            public bool IsUpdatedAt { get; set; }
            public DefaultValueDefinition? DefaultValue { get; set; }

            /// <summary>
            /// Captures parsed field state prior to validation and RelationInfo resolution.
            /// </summary>
            public FieldBuilder(string name, string rawType, bool isList, bool isOptional, List<string> attributes, List<string> relationAttributes, TextSpan span)
            {
                Name = name;
                RawType = rawType;
                IsList = isList;
                IsOptional = isOptional;
                Attributes = attributes;
                RelationAttributes = relationAttributes;
                Span = span;
            }
        }
    }
}
