using System;
using System.Collections.Generic;
using Xunit;

namespace Charisma.Schema.Tests;

public sealed class SchemaHardeningTests
{
    [Fact]
    public void Constructor_NullModels_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new CharismaSchema(
            null!,
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>()));
    }

    [Fact]
    public void Constructor_NullEnums_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal),
            null!,
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>()));
    }

    [Fact]
    public void Constructor_NullDatasources_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal),
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            null!,
            new List<GeneratorDefinition>()));
    }

    [Fact]
    public void Constructor_NullGenerators_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal),
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            null!));
    }

    [Fact]
    public void Normalize_NullSchema_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => SchemaNormalizer.Normalize(null!));
    }

    [Fact]
    public void Normalize_IsDeterministic_ForEquivalentSchemas()
    {
        var schemaA = TestSchemas.Basic();
        var schemaB = TestSchemas.Basic();
        Assert.Equal(schemaA.CanonicalText, schemaB.CanonicalText);
        Assert.Equal(schemaA.SchemaHash, schemaB.SchemaHash);
    }

    [Fact]
    public void Normalize_IgnoresFieldDeclarationOrder_WithinModel()
    {
        var schemaA = TestSchemas.Basic();
        var schemaB = TestSchemas.BasicReorderedFields();
        Assert.Equal(schemaA.CanonicalText, schemaB.CanonicalText);
        Assert.Equal(schemaA.SchemaHash, schemaB.SchemaHash);
    }

    [Fact]
    public void Normalize_IgnoresDatasourceAndGeneratorDeclarationOrder()
    {
        var schemaA = TestSchemas.WithInfra(orderSwap: false);
        var schemaB = TestSchemas.WithInfra(orderSwap: true);

        Assert.Equal(schemaA.CanonicalText, schemaB.CanonicalText);
        Assert.Equal(schemaA.SchemaHash, schemaB.SchemaHash);
    }

    [Fact]
    public void Normalize_ModelWithNoFields_DoesNotThrow_AndStillRendersModelBlock()
    {
        var schema = new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["Audit"] = new ModelDefinition("Audit", Array.Empty<FieldDefinition>(), Array.Empty<string>())
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());

        var canonical = schema.CanonicalText.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("model Audit {\n}", canonical, StringComparison.Ordinal);
    }

    [Fact]
    public void Normalize_RelationInfo_EmitsTypedRelationAttribute()
    {
        var rel = new RelationFieldDefinition(
            "Owner",
            "User",
            isList: false,
            isOptional: false,
            attributes: Array.Empty<string>(),
            relationAttributes: Array.Empty<string>(),
            relationInfo: new RelationInfo(
                foreignModel: "User",
                localFields: new[] { "OwnerId" },
                foreignFields: new[] { "Id" },
                isCollection: false,
                relationName: "OwnedBy",
                onDelete: OnDeleteBehavior.Cascade));

        var schema = new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["Task"] = new ModelDefinition(
                    "Task",
                    new FieldDefinition[]
                    {
                        new ScalarFieldDefinition("Id", "Id", false, false, new[] { "@id" }, isId: true),
                        new ScalarFieldDefinition("OwnerId", "Id", false, false, Array.Empty<string>()),
                        rel
                    },
                    Array.Empty<string>())
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());

        Assert.Contains("@relation(fk: [Task.OwnerId], pk: [User.Id], name: \"OwnedBy\", onDelete: Cascade)", schema.CanonicalText, StringComparison.Ordinal);
    }

    [Fact]
    public void SchemaHash_IsLowerHexSha256()
    {
        var schema = TestSchemas.Basic();
        Assert.Matches("^[0-9a-f]{64}$", schema.SchemaHash);
    }

    [Fact]
    public void ModelDefinition_GetField_IsCaseSensitive()
    {
        var model = new ModelDefinition(
            "User",
            new FieldDefinition[]
            {
                new ScalarFieldDefinition("Name", "String", false, false, Array.Empty<string>())
            },
            Array.Empty<string>());

        Assert.NotNull(model.GetField("Name"));
        Assert.Null(model.GetField("name"));
    }
}

internal static class TestSchemas
{
    public static CharismaSchema Basic()
    {
        return new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["User"] = new ModelDefinition(
                    "User",
                    new FieldDefinition[]
                    {
                        new ScalarFieldDefinition("Id", "Int", false, false, new[] { "@id" }, isId: true),
                        new ScalarFieldDefinition("Name", "String", false, false, Array.Empty<string>())
                    },
                    Array.Empty<string>())
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());
    }

    public static CharismaSchema BasicReorderedFields()
    {
        return new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["User"] = new ModelDefinition(
                    "User",
                    new FieldDefinition[]
                    {
                        new ScalarFieldDefinition("Name", "String", false, false, Array.Empty<string>()),
                        new ScalarFieldDefinition("Id", "Int", false, false, new[] { "@id" }, isId: true)
                    },
                    Array.Empty<string>())
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());
    }

    public static CharismaSchema WithInfra(bool orderSwap)
    {
        var first = new DatasourceDefinition(
            "alpha",
            "postgresql",
            "env(\"DB_ALPHA\")",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["schema"] = "a",
                ["sslmode"] = "require"
            });

        var second = new DatasourceDefinition(
            "omega",
            "postgresql",
            "env(\"DB_OMEGA\")",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["schema"] = "z",
                ["sslmode"] = "prefer"
            });

        var g1 = new GeneratorDefinition(
            "aClient",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["provider"] = "charisma-client",
                ["output"] = "./A"
            });

        var g2 = new GeneratorDefinition(
            "zClient",
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["provider"] = "charisma-client",
                ["output"] = "./Z"
            });

        var datasources = orderSwap
            ? new List<DatasourceDefinition> { second, first }
            : new List<DatasourceDefinition> { first, second };

        var generators = orderSwap
            ? new List<GeneratorDefinition> { g2, g1 }
            : new List<GeneratorDefinition> { g1, g2 };

        return new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["User"] = new ModelDefinition(
                    "User",
                    new FieldDefinition[]
                    {
                        new ScalarFieldDefinition("Id", "Id", false, false, new[] { "@id" }, isId: true)
                    },
                    Array.Empty<string>())
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            datasources,
            generators);
    }
}
