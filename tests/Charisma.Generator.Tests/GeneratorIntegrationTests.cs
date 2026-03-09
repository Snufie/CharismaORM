// using System;
// using System.IO;
// using System.Linq;
// using Charisma.Generator;
// using Charisma.Parser;

// namespace Charisma.Generator.Tests;

// public class GeneratorIntegrationTests
// {
//     [Fact]
//     public void Generate_FromRootSchema_WritesOutputs()
//     {
//         var projectDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
//         var repoRoot = Path.GetFullPath(Path.Combine(projectDir, "..", ".."));
//         var schemaPath = Path.Combine(repoRoot, "schema.charisma");
//         var outputDir = Path.Combine(projectDir, "Generated");

//         if (Directory.Exists(outputDir))
//         {
//             Directory.Delete(outputDir, recursive: true);
//         }

//         var parser = new RoslynSchemaParser();
//         var schemaText = File.ReadAllText(schemaPath);
//         var schema = parser.Parse(schemaText);

//         var generator = new CharismaGenerator(new GeneratorOptions
//         {
//             RootNamespace = "Charisma.Generated",
//             GeneratorVersion = "test",
//             OutputDirectory = outputDir
//         });

//         var units = generator.Generate(schema);

//         Assert.NotEmpty(units);
//         Assert.True(Directory.Exists(outputDir));

//         var generatedFiles = Directory.GetFiles(outputDir, "*.cs", SearchOption.AllDirectories);
//         Assert.NotEmpty(generatedFiles);

//         // basic sanity: ensure the schema hash header was written
//         var sample = File.ReadAllText(generatedFiles[0]);
//         Assert.Contains(schema.SchemaHash, sample);
//     }
// }
