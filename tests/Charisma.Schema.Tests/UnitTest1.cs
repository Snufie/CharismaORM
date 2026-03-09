using System.Collections.Generic;
using Xunit;
using Charisma.Schema;


namespace Charisma.Schema.Tests
{
    public sealed class SchemaNormalizerTests
    {
        [Fact]
        public void Normalize_IsDeterministic()
        {
            var schema1 = TestSchemas.Basic();
            var schema2 = TestSchemas.Basic();

            Assert.Equal(schema1.CanonicalText, schema2.CanonicalText);
        }

        [Fact]
        public void Normalize_IgnoresDeclarationOrder()
        {
            var schemaA = TestSchemas.Basic();
            var schemaB = TestSchemas.BasicReordered();

            Assert.Equal(schemaA.CanonicalText, schemaB.CanonicalText);
        }
    }

    internal static class TestSchemas
    {
        public static CharismaSchema Basic()
        {
            return new CharismaSchema(
                new Dictionary<string, ModelDefinition>
                {
                    ["User"] = new ModelDefinition(
                        "User",
                        new[]
                        {
                            new ScalarFieldDefinition("Id", "Int", false, false, new[] { "@id" }),
                            new ScalarFieldDefinition("Name", "String", false, false, new string[0])
                        },
                        new string[0])
                },
                new Dictionary<string, EnumDefinition>(), new List<DatasourceDefinition>(), new List<GeneratorDefinition>());
        }

        public static CharismaSchema BasicReordered()
        {
            return new CharismaSchema(
                new Dictionary<string, ModelDefinition>
                {
                    ["User"] = new ModelDefinition(
                        "User",
                        new[]
                        {
                            new ScalarFieldDefinition("Name", "String", false, false, new string[0]),
                            new ScalarFieldDefinition("Id", "Int", false, false, new[] { "@id" })
                        },
                        new string[0])
                },
                new Dictionary<string, EnumDefinition>(), new List<DatasourceDefinition>(), new List<GeneratorDefinition>());
        }
    }
}
