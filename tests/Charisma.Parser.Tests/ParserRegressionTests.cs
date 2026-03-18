using System.Linq;
using Charisma.Parser;
using Charisma.Schema;
using Xunit;

namespace Charisma.Parser.Tests;

public sealed class ParserRegressionTests
{
    private readonly ISchemaParser _parser = new RoslynSchemaParser();

    [Fact]
    public void Regression_BracesInsideDefaultLiteral_DoNotTerminateModelBlock()
    {
        var schema = _parser.Parse("""
            model A {
              id Id @id @default(uuid())
              payload Json @default("{}")
              created DateTime @default(now())
            }
            """);

        var model = schema.Models["A"];
        var payload = Assert.IsType<ScalarFieldDefinition>(model.Fields.First(f => f.Name == "payload"));
        var created = Assert.IsType<ScalarFieldDefinition>(model.Fields.First(f => f.Name == "created"));

        Assert.Equal(DefaultValueKind.Json, payload.DefaultValue?.Kind);
        Assert.Equal(DefaultValueKind.Now, created.DefaultValue?.Kind);
    }

    [Fact]
    public void Regression_JsonDefaultOnNonJsonField_IsRejectedEvenWithBraces()
    {
        var ex = Assert.Throws<CharismaSchemaAggregateException>(() => _parser.Parse("""
            model A {
              id Int @id
              bad String @default("{}")
            }
            """));

        Assert.Contains(ex.Errors, e => e.Message.Contains("JSON default on field 'bad'", System.StringComparison.OrdinalIgnoreCase));
    }
}
