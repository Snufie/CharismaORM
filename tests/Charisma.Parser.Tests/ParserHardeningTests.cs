using System;
using System.Linq;
using Charisma.Parser;
using Charisma.Schema;
using Xunit;

namespace Charisma.Parser.Tests;

public sealed class ParserHardeningTests
{
    private readonly ISchemaParser _parser = new RoslynSchemaParser();

    [Fact]
    public void Parse_NullSchema_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _parser.Parse(null!));
    }

    [Fact]
    public void Parse_UnexpectedTopLevelToken_ThrowsAggregate()
    {
        var ex = ParseShouldFail("wat");
        AssertHasError(ex, "Unexpected top-level token");
    }

    [Fact]
    public void Parse_DuplicateEnum_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            enum A {
              X
            }
            enum A {
              Y
            }
            model M {
              id Int @id
            }
            """);

        AssertHasError(ex, "Duplicate enum 'A'");
    }

    [Fact]
    public void Parse_DatasourceMissingProvider_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            datasource db {
              url = env("DB_URL")
            }

            model A {
              id Int @id
            }
            """);

        AssertHasError(ex, "Datasource 'db' is missing provider");
    }

    [Fact]
    public void Parse_DatasourceMissingUrl_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            datasource db {
              provider = "postgresql"
            }

            model A {
              id Int @id
            }
            """);

        AssertHasError(ex, "Datasource 'db' is missing url");
    }

    [Fact]
    public void Parse_UnterminatedGenerator_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            generator client {
              provider = "charisma-client"

            model A {
              id Int @id
            }
            """);

        AssertHasError(ex, "Unterminated generator 'client'");
    }

    [Fact]
    public void Parse_DuplicateField_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              id String
            }
            """);

        AssertHasError(ex, "Duplicate field 'id'");
    }

    [Fact]
    public void Parse_IdOnOptionalField_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int? @id
            }
            """);

        AssertHasError(ex, "cannot be optional");
    }

    [Fact]
    public void Parse_IdOnListField_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              ids Int[] @id
            }
            """);

        AssertHasError(ex, "cannot be a list");
    }

    [Fact]
    public void Parse_UpdatedAtRequiresDateTime_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              bad String @updatedAt
            }
            """);

        AssertHasError(ex, "must be of type DateTime");
    }

    [Fact]
    public void Parse_DefaultWithoutValue_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              x Int @default()
            }
            """);

        AssertHasError(ex, "requires a value");
    }

    [Fact]
    public void Parse_DefaultAutoincrementWrongType_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              x String @default(autoincrement())
            }
            """);

        AssertHasError(ex, "autoincrement() default on field 'x'");
    }

    [Fact]
    public void Parse_DefaultAutoincrementOptional_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              x Int? @default(autoincrement())
            }
            """);

        AssertHasError(ex, "cannot be optional");
    }

    [Fact]
    public void Parse_DefaultUuidWrongType_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id @default(uuid())
            }
            """);

        AssertHasError(ex, "requires UUID or Id type");
    }

    [Fact]
    public void Parse_DefaultNowWrongType_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              bad String @default(now())
            }
            """);

        AssertHasError(ex, "requires DateTime type");
    }

    [Fact]
    public void Parse_DefaultJsonWrongType_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              bad String @default("{}")
            }
            """);

        AssertHasError(ex, "JSON default on field 'bad'");
    }

    [Fact]
    public void Parse_DefaultDbgeneratedUnsupported_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              x Int @default(dbgenerated())
            }
            """);

        AssertHasError(ex, "dbgenerated() defaults are not supported");
    }

    [Fact]
    public void Parse_ModelDirectivesWithEmptyFields_ThrowAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              @@id()
              @@unique()
              @@index()
            }
            """);

        AssertHasError(ex, "@@id on model 'A' requires fields");
        AssertHasError(ex, "@@unique on model 'A' requires fields");
        AssertHasError(ex, "@@index on model 'A' requires fields");
    }

    [Fact]
    public void Parse_ConflictingPrimaryKeyDeclarations_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              another Int
              @@id(another)
            }
            """);

        AssertHasError(ex, "Conflicting primary key declarations on model 'A'");
    }

    [Fact]
    public void Parse_RelationUnsupportedOnDeleteBehavior_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              bId Int
              b B @relation(fk: (A.bId), pk: (B.id), onDelete: ""explode"")
            }

            model B {
              id Int @id
            }
            """);

        AssertHasError(ex, "Unsupported onDelete behavior");
    }

    [Fact]
    public void Parse_RelationEndpointWhitespace_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              bId Int
              b B @relation(fk: (A. bId), pk: (B.id))
            }

            model B {
              id Int @id
            }
            """);

        AssertHasError(ex, "contains invalid whitespace");
    }

    [Fact]
    public void Parse_RelationMultipleFkEntries_ThrowsAggregate()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int @id
              b1 Int
              b2 Int
              b B @relation(fk: (A.b1, A.b2), pk: (B.id, B.id))
            }

            model B {
              id Int @id
            }
            """);

        AssertHasError(ex, "Multiple fk(...) entries are not supported");
    }

    [Fact]
    public void Parse_CollectsMultipleErrorsInSinglePass()
    {
        var ex = ParseShouldFail("""
            model A {
              id Int? @id
              updated String @updatedAt
            }
            """);

        Assert.True(ex.Errors.Count >= 2, "Expected at least two independent diagnostics.");
    }

    [Fact]
    public void Parse_ValidDatasourceGeneratorAndDefaults_ArePreserved()
    {
        var schema = _parser.Parse("""
            datasource db {
              provider = "postgresql"
              url = env("DB_URL")
              schema = "public"
            }

            generator client {
              provider = "charisma-client"
              output = "./Generated"
            }

            model A {
              id Id @id @default(uuid())
              created DateTime @default(now())
              payload Json @default("{}")
            }
            """);

        Assert.Single(schema.Datasources);
        Assert.Single(schema.Generators);

        var model = schema.Models["A"];
        var id = Assert.IsType<ScalarFieldDefinition>(model.Fields.First(f => f.Name == "id"));
        var created = Assert.IsType<ScalarFieldDefinition>(model.Fields.First(f => f.Name == "created"));
        var payload = Assert.IsType<ScalarFieldDefinition>(model.Fields.First(f => f.Name == "payload"));

        Assert.Equal(DefaultValueKind.UuidV4, id.DefaultValue?.Kind);
        Assert.Equal(DefaultValueKind.Now, created.DefaultValue?.Kind);
        Assert.Equal(DefaultValueKind.Json, payload.DefaultValue?.Kind);
    }

    [Fact]
    public void Parse_UuidV7Default_IsRecognized()
    {
        var schema = _parser.Parse("""
          model A {
            id Id @id @default(uuidv7())
          }
          """);

        var model = schema.Models["A"];
        var id = Assert.IsType<ScalarFieldDefinition>(model.Fields.First(f => f.Name == "id"));
        Assert.Equal(DefaultValueKind.UuidV7, id.DefaultValue?.Kind);
    }

    [Fact]
    public void Parse_UuidV7WithCreatedAt_IsRejected()
    {
        var ex = ParseShouldFail("""
          model A {
            id Id @id @default(uuidv7())
            created_at DateTime @default(now())
          }
          """);

        AssertHasError(ex, "uses UUIDv7 primary keys");
    }

    private CharismaSchemaAggregateException ParseShouldFail(string schema)
    {
        return Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse(schema));
    }

    private static void AssertHasError(CharismaSchemaAggregateException ex, string expectedSnippet)
    {
        Assert.Contains(ex.Errors, e => e.Message.Contains(expectedSnippet, StringComparison.OrdinalIgnoreCase));
    }
}
