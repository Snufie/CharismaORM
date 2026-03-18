using Charisma.Schema;

namespace Charisma.Generator.Tests;

public sealed class ClientTest
{
    [Fact]
    public void CharismaGenerator_NullRootNamespace_Throws()
    {
        using var temp = new TempDirectory();
        var options = new GeneratorOptions
        {
            RootNamespace = null!,
            GeneratorVersion = "tests",
            OutputDirectory = temp.Path
        };

        Assert.Throws<ArgumentNullException>(() => new CharismaGenerator(options));
    }

    [Fact]
    public void Generate_EmptySchema_StillProducesCoreArtifacts()
    {
        using var temp = new TempDirectory();
        var generator = new CharismaGenerator(new GeneratorOptions
        {
            RootNamespace = "Charisma.Generated",
            GeneratorVersion = "tests",
            OutputDirectory = temp.Path
        });

        var schema = new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal),
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());

        var units = generator.Generate(schema);
        Assert.NotEmpty(units);
        Assert.True(File.Exists(Path.Combine(temp.Path, "CharismaClient.g.cs")));
        Assert.True(File.Exists(Path.Combine(temp.Path, "Filters", "StringFilter.g.cs")));
    }
}

