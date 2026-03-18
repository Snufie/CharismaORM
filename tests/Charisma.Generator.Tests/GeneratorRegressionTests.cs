using System.Text.RegularExpressions;
using Charisma.Parser;

namespace Charisma.Generator.Tests;

public sealed class GeneratorRegressionTests
{
    [Fact]
    public void Regression_NonNullableEnumArgs_MustNotBeInitializedToNull()
    {
        var projectDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
        var repoRoot = Path.GetFullPath(Path.Combine(projectDir, "..", ".."));
        var schemaPath = Path.Combine(repoRoot, "schema.charisma");

        using var temp = new TempDirectory();

        var parser = new RoslynSchemaParser();
        var schema = parser.Parse(File.ReadAllText(schemaPath));

        var generator = new CharismaGenerator(new GeneratorOptions
        {
            RootNamespace = "Charisma.Generated",
            GeneratorVersion = "tests",
            OutputDirectory = temp.Path
        });

        generator.Generate(schema);

        var robotArgs = File.ReadAllText(Path.Combine(temp.Path, "Args", "RobotArgs.g.cs"));
        var sensorArgs = File.ReadAllText(Path.Combine(temp.Path, "Args", "SensorArgs.g.cs"));

        AssertDoesNotAssignNull(robotArgs, "LaadStatusEnum", "LaadStatus");
        AssertDoesNotAssignNull(sensorArgs, "SensorStatusEnum", "Status");
        AssertDoesNotAssignNull(sensorArgs, "SensorTypeEnum", "Type");
    }

    private static void AssertDoesNotAssignNull(string code, string enumType, string propertyName)
    {
        var pattern = $@"public\s+{Regex.Escape(enumType)}\s+{Regex.Escape(propertyName)}\s*\{{\s*get;\s*set;\s*\}}\s*=\s*null\s*;";
        Assert.DoesNotMatch(new Regex(pattern, RegexOptions.Singleline), code);
    }
}
