using Charisma.Schema;

namespace Charisma.Migration.Tests;

public sealed class SchemaFileWriterTests
{
    [Fact]
    public async Task WriteAsync_CreatesDirectoryAndWritesContent()
    {
        using var temp = new TempDirectory();
        var writer = new SchemaFileWriter();
        var schemaPath = Path.Combine(temp.Path, "nested", "schema.charisma");
        var schema = BuildSchema();

        var result = await writer.WriteAsync(schema, schemaPath, overwrite: false);

        Assert.True(result.Written);
        Assert.False(result.Skipped);
        Assert.True(File.Exists(schemaPath));
        Assert.Equal(schema.CanonicalText, await File.ReadAllTextAsync(schemaPath));
    }

    [Fact]
    public async Task WriteAsync_WhenUnchanged_ReturnsSkipped()
    {
        using var temp = new TempDirectory();
        var writer = new SchemaFileWriter();
        var schemaPath = Path.Combine(temp.Path, "schema.charisma");
        var schema = BuildSchema();
        await File.WriteAllTextAsync(schemaPath, schema.CanonicalText);

        var result = await writer.WriteAsync(schema, schemaPath, overwrite: false);

        Assert.False(result.Written);
        Assert.True(result.Skipped);
        Assert.Equal("Content unchanged", result.Reason);
    }

    [Fact]
    public async Task WriteAsync_WhenDifferentAndOverwriteFalse_ReturnsSkipped()
    {
        using var temp = new TempDirectory();
        var writer = new SchemaFileWriter();
        var schemaPath = Path.Combine(temp.Path, "schema.charisma");
        var schema = BuildSchema();
        await File.WriteAllTextAsync(schemaPath, "model X { id Int @id }");

        var result = await writer.WriteAsync(schema, schemaPath, overwrite: false);

        Assert.False(result.Written);
        Assert.True(result.Skipped);
        Assert.Equal("File exists and overwrite is disabled", result.Reason);
    }

    private static CharismaSchema BuildSchema()
    {
        return new CharismaSchema(
            new Dictionary<string, ModelDefinition>(StringComparer.Ordinal)
            {
                ["User"] = new ModelDefinition(
                    "User",
                    new FieldDefinition[]
                    {
                        new ScalarFieldDefinition("id", "Int", false, false, new[] { "@id" }, isId: true)
                    },
                    Array.Empty<string>())
            },
            new Dictionary<string, EnumDefinition>(StringComparer.Ordinal),
            new List<DatasourceDefinition>(),
            new List<GeneratorDefinition>());
    }
}

internal sealed class TempDirectory : IDisposable
{
    public TempDirectory()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "charisma-migration-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(Path))
            {
                Directory.Delete(Path, recursive: true);
            }
        }
        catch
        {
            // Best-effort cleanup.
        }
    }
}
