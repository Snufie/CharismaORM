using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Charisma.Generator;
using Charisma.Migration;
using Charisma.Migration.Postgres;
using Charisma.Migration.Introspection.Push.Postgres;
using Charisma.Parser;
using Charisma.Schema;
using Charisma.Runtime;

namespace Charisma.Client;

/// <summary>
/// Command-line entry point for generating Charisma client code from a schema.
/// </summary>
public static class Program
{
    // Hardcoded generator version for headers.
    private const string GeneratorVersion = "1.8.0";

    private sealed record ClientConfig(string? ConnectionString, string RootNamespace, string OutputDirectory);

    // Entry: charisma generate <schemaPath> <outputPath?> [--root-namespace MyApp.Generated]
    /// <summary>
    /// Executes the CLI to generate code from a schema file.
    /// </summary>
    /// <param name="args">Command-line arguments.</param>
    /// <returns>Exit code: 0 on success, non-zero on usage or errors.</returns>
    public static int Main(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 0;
        }

        var command = args[0].ToLowerInvariant();

        if (command is "-h" or "--help" or "help")
        {
            PrintUsage();
            return 0;
        }

        return command switch
        {
            "generate" => HandleGenerate(args),
            "db" when args.Length >= 2 && args[1].Equals("pull", StringComparison.OrdinalIgnoreCase) => HandleDbPull(args),
            "db" when args.Length >= 2 && args[1].Equals("push", StringComparison.OrdinalIgnoreCase) => HandleDbPush(args),
            "migrate" => HandleMigrate(args),
            _ => FailUsage()
        };
    }

    private static int HandleGenerate(string[] args)
    {
        var schemaPath = args.Length >= 2 && !args[1].StartsWith("--", StringComparison.OrdinalIgnoreCase)
            ? args[1]
            : Path.Combine(Directory.GetCurrentDirectory(), "schema.charisma");

        if (!File.Exists(schemaPath))
        {
            CliConsole.Error($"Schema file not found: {schemaPath}");
            return 1;
        }

        // Optional positional output override; otherwise use generator config or default.
        string? outputOverride = args.Length >= 3 && !args[2].StartsWith("--", StringComparison.OrdinalIgnoreCase)
            ? args[2]
            : null;

        // Optional root namespace override via flag; otherwise generator or default.
        string? rootNamespaceOverride = ParseRootNamespaceArg(args);

        try
        {
            var schemaText = File.ReadAllText(schemaPath);

            // Phase 0 parser/validator (assumed available from Phase 0).
            RoslynSchemaParser parser = new();
            var schema = parser.Parse(schemaText);

            var opts = BuildClientConfig(schema, connectionOverride: null, rootNamespaceOverride, outputOverride);
            var finalOutput = opts.OutputDirectory;
            var finalRoot = opts.RootNamespace;

            var generator = new CharismaGenerator(
                new GeneratorOptions
                {
                    RootNamespace = finalRoot,
                    GeneratorVersion = GeneratorVersion,
                    OutputDirectory = finalOutput
                });

            generator.Generate(schema);

            CliConsole.Success($"Generation completed. Output: {finalOutput}");
            return 0;
        }
        catch (Exception ex)
        {
            CliConsole.Error(ex.ToString());
            return 1;
        }
    }

    /// <summary>
    /// Writes usage information to standard output.
    /// </summary>
    private static int HandleDbPull(string[] args)
    {
        var parsed = ParseDbPullArgs(args);
        if (!parsed.Success)
        {
            return FailUsage();
        }

        var schemaPath = parsed.SchemaPath;
        CharismaSchema? existingSchema = TryParseSchema(schemaPath);

        var connectionString = ResolveConnectionString(parsed.ConnectionString, existingSchema)
                               ?? TryExtractDatasourceUrl(schemaPath);
        if (connectionString is null)
        {
            Console.Error.WriteLine("Connection string is required (provide --connection or set it in datasource url/env in the schema file).");
            return 1;
        }

        try
        {
            var options = new PostgresIntrospectionOptions(connectionString);
            var introspector = new PostgresSchemaIntrospector(options, existingSchema);
            var schema = introspector.IntrospectAsync().GetAwaiter().GetResult();

            bool schemaIsEmpty = existingSchema is null
                                  || (existingSchema.Models.Count == 0 && existingSchema.Enums.Count == 0);

            // If the file doesn't exist, is empty (only datasource/generator), or --force, write full schema.
            if (!File.Exists(schemaPath) || parsed.Force || schemaIsEmpty)
            {
                var writer = new SchemaFileWriter();
                var result = writer.WriteAsync(schema, schemaPath, overwrite: true, CancellationToken.None).GetAwaiter().GetResult();
                Console.WriteLine(result.Written
                    ? $"Schema created at {schemaPath}"
                    : $"Schema unchanged ({result.Reason ?? "no-op"})");
                return 0;
            }

            if (parsed.Force)
            {
                var writer = new SchemaFileWriter();
                var result = writer.WriteAsync(schema, schemaPath, overwrite: true, CancellationToken.None).GetAwaiter().GetResult();
                Console.WriteLine(result.Written
                    ? $"Schema rewritten at {schemaPath}"
                    : $"Schema unchanged ({result.Reason ?? "no-op"})");
                return 0;
            }

            // Update-only: replace datasource block, leave rest intact.
            var existing = File.ReadAllText(schemaPath);
            var updated = ReplaceDatasourceBlock(existing, schema);

            if (updated == existing)
            {
                Console.WriteLine("Schema unchanged (datasource already up to date)");
                return 0;
            }

            File.WriteAllText(schemaPath, updated);
            Console.WriteLine($"Schema updated (datasource) at {schemaPath}");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.ToString());
            return 1;
        }
    }

    private static int HandleDbPush(string[] args)
    {
        var parsed = ParseDbPushArgs(args);
        if (!parsed.Success)
        {
            return FailUsage();
        }

        if (!File.Exists(parsed.SchemaPath))
        {
            Console.Error.WriteLine($"Schema file not found: {parsed.SchemaPath}");
            return 1;
        }

        try
        {
            var schemaText = File.ReadAllText(parsed.SchemaPath);
            RoslynSchemaParser parser = new();
            var schema = parser.Parse(schemaText);

            var connectionString = ResolveConnectionString(parsed.ConnectionString, schema);
            if (connectionString is null)
            {
                Console.Error.WriteLine("Connection string is required (provide --connection or set it in datasource url/env in the schema file).");
                return 1;
            }

            if (parsed.ForceReset)
            {
                var resetter = new PostgresDatabaseResetter(connectionString);
                resetter.ResetAsync().GetAwaiter().GetResult();

                var pusher = new PostgresSchemaPusher(new PostgresPushOptions(connectionString));
                pusher.PushAsync(schema).GetAwaiter().GetResult();

                Console.WriteLine("Database reset and recreated from schema.");
                return 0;
            }

            var allowDataLoss = parsed.AllowDataLoss;
            var nonInteractive = parsed.NonInteractive;
            var emitSqlPath = parsed.EmitSqlPath;
            var planOnly = parsed.PlanOnly;

            var options = new PostgresMigrationOptions(allowDestructive: allowDataLoss, allowDataLoss: allowDataLoss);
            var planner = new PostgresMigrationPlanner(new PostgresIntrospectionOptions(connectionString), options);
            var plan = planner.PlanAsync(schema).GetAwaiter().GetResult();

            if (emitSqlPath is not null)
            {
                var sqlText = string.Join("\n", plan.Steps.Select(s => s.Sql).Where(s => !string.IsNullOrWhiteSpace(s)));
                File.WriteAllText(emitSqlPath, sqlText);
            }

            // Preview summary
            Console.WriteLine($"Plan: {plan.Steps.Count} step(s), {plan.Steps.Count(s => s.IsDestructive)} destructive, {plan.Warnings.Count} warning(s), {plan.Unexecutable.Count} unexecutable.");
            for (int i = 0; i < plan.Steps.Count; i++)
            {
                var step = plan.Steps[i];
                var marker = step.IsDestructive ? "!" : "-";
                Console.WriteLine($"  [{i + 1}] {marker} {step.Description}");
            }

            if (plan.HasUnexecutable)
            {
                Console.WriteLine("Found unexecutable changes:");
                foreach (var msg in plan.Unexecutable)
                {
                    Console.WriteLine($"- {msg}");
                }
                Console.WriteLine("Use --force-reset to recreate the database.");
                return 1;
            }

            if (plan.HasWarnings && !allowDataLoss)
            {
                Console.WriteLine("⚠️  Data loss warnings:");
                foreach (var w in plan.Warnings)
                {
                    Console.WriteLine($"  • {w}");
                }

                if (!nonInteractive && !Console.IsInputRedirected)
                {
                    Console.Write("Proceed anyway? [y/N]: ");
                    var key = Console.ReadLine();
                    if (string.Equals(key, "y", StringComparison.OrdinalIgnoreCase) || string.Equals(key, "yes", StringComparison.OrdinalIgnoreCase))
                    {
                        allowDataLoss = true;
                        options = new PostgresMigrationOptions(allowDestructive: allowDataLoss, allowDataLoss: allowDataLoss);
                    }
                    else
                    {
                        Console.WriteLine("Push cancelled.");
                        return 1;
                    }
                }
                else
                {
                    Console.WriteLine("Non-interactive mode with warnings present; aborting. Use --accept-data-loss to override.");
                    return 1;
                }
            }

            if (planOnly)
            {
                Console.WriteLine("Plan only flag set; no changes applied.");
                return 0;
            }

            if (plan.Steps.Count == 0)
            {
                Console.WriteLine("Database is already in sync with schema.");
                return 0;
            }

            var runner = new PostgresMigrationRunner(connectionString);
            runner.ExecuteAsync(plan, options).GetAwaiter().GetResult();

            Console.WriteLine("db push completed successfully.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.ToString());
            return 1;
        }
    }

    private static int HandleMigrate(string[] args)
    {
        // Usage: charisma migrate <schemaPath?> [--connection <conn>]
        var schemaPath = args.Length >= 2 && !args[1].StartsWith("--", StringComparison.OrdinalIgnoreCase)
            ? args[1]
            : Path.Combine(Directory.GetCurrentDirectory(), "schema.charisma");

        if (!File.Exists(schemaPath))
        {
            Console.Error.WriteLine($"Schema file not found: {schemaPath}");
            return 1;
        }

        string? connectionString = null;
        for (int i = 2; i < args.Length - 1; i++)
        {
            if (args[i] == "--connection" && i + 1 < args.Length)
            {
                connectionString = args[i + 1];
                break;
            }
        }

        var schemaText = File.ReadAllText(schemaPath);
        RoslynSchemaParser parser = new();
        var schema = parser.Parse(schemaText);
        connectionString ??= ResolveConnectionString(null, schema);
        if (connectionString is null)
        {
            Console.Error.WriteLine("Connection string is required (provide --connection or set it in datasource url/env in the schema file).");
            return 1;
        }

        try
        {
            var options = new PostgresMigrationOptions(allowDestructive: true, allowDataLoss: true);
            var planner = new PostgresMigrationPlanner(new PostgresIntrospectionOptions(connectionString), options);
            var plan = planner.PlanAsync(schema).GetAwaiter().GetResult();

            Console.WriteLine($"Migration plan: {plan.Steps.Count} step(s), {plan.Steps.Count(s => s.IsDestructive)} destructive, {plan.Warnings.Count} warning(s), {plan.Unexecutable.Count} unexecutable.");
            for (int i = 0; i < plan.Steps.Count; i++)
            {
                var step = plan.Steps[i];
                var marker = step.IsDestructive ? "!" : "-";
                Console.WriteLine($"  [{i + 1}] {marker} {step.Description}");
            }

            if (plan.HasUnexecutable)
            {
                Console.WriteLine("Found unexecutable changes:");
                foreach (var msg in plan.Unexecutable)
                {
                    Console.WriteLine($"- {msg}");
                }
                Console.WriteLine("Migration cannot proceed. Use db push --force-reset to recreate the database if needed.");
                return 1;
            }

            if (plan.Steps.Count == 0)
            {
                Console.WriteLine("Database is already in sync with schema. No migration needed.");
                return 0;
            }

            var runner = new PostgresMigrationRunner(connectionString);
            runner.ExecuteAsync(plan, options).GetAwaiter().GetResult();

            Console.WriteLine("Migration completed successfully.");
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.ToString());
            return 1;
        }
    }

    private static (bool Success, string SchemaPath, string? ConnectionString, bool Force) ParseDbPullArgs(string[] args)
    {
        // Expected: charisma db pull [schemaPath] [--connection <conn>] [--force]
        string schemaPath = Path.Combine(Directory.GetCurrentDirectory(), "schema.charisma");
        string? connection = null;
        bool force = args.Any(a => string.Equals(a, "--force", StringComparison.OrdinalIgnoreCase));

        for (int i = 2; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg.Equals("--force", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (arg.Equals("--connection", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= args.Length)
                {
                    CliConsole.Error("--connection requires a value");
                    return (false, schemaPath, null, force);
                }
                connection = args[i + 1];
                i++;
                continue;
            }

            // First non-flag after "db pull" is schemaPath
            schemaPath = arg;
        }

        return (true, schemaPath, connection, force);
    }

    private static (bool Success, string SchemaPath, string? ConnectionString, bool ForceReset, bool AllowDataLoss, bool NonInteractive, string? EmitSqlPath, bool PlanOnly) ParseDbPushArgs(string[] args)
    {
        // Expected: charisma db push [schemaPath] [--connection <conn>] [--force-reset] [--accept-data-loss] [--yes] [--emit-sql <file>] [--plan-only]
        string schemaPath = Path.Combine(Directory.GetCurrentDirectory(), "schema.charisma");
        string? connection = null;
        bool forceReset = args.Any(a => string.Equals(a, "--force-reset", StringComparison.OrdinalIgnoreCase));
        bool allowDataLoss = args.Any(a => string.Equals(a, "--accept-data-loss", StringComparison.OrdinalIgnoreCase));
        bool nonInteractive = args.Any(a => string.Equals(a, "--yes", StringComparison.OrdinalIgnoreCase));
        string? emitSqlPath = null;
        bool planOnly = args.Any(a => string.Equals(a, "--plan-only", StringComparison.OrdinalIgnoreCase));

        for (int i = 2; i < args.Length; i++)
        {
            var arg = args[i];

            if (arg.Equals("--force-reset", StringComparison.OrdinalIgnoreCase) || arg.Equals("--accept-data-loss", StringComparison.OrdinalIgnoreCase) || arg.Equals("--yes", StringComparison.OrdinalIgnoreCase) || arg.Equals("--plan-only", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (arg.Equals("--emit-sql", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= args.Length)
                {
                    CliConsole.Error("--emit-sql requires a value");
                    return (false, schemaPath, null, forceReset, allowDataLoss, nonInteractive, emitSqlPath, planOnly);
                }
                emitSqlPath = args[i + 1];
                i++;
                continue;
            }

            if (arg.Equals("--connection", StringComparison.OrdinalIgnoreCase))
            {
                if (i + 1 >= args.Length)
                {
                    CliConsole.Error("--connection requires a value");
                    return (false, schemaPath, null, forceReset, allowDataLoss, nonInteractive, emitSqlPath, planOnly);
                }
                connection = args[i + 1];
                i++;
                continue;
            }

            schemaPath = arg;
        }

        return (true, schemaPath, connection, forceReset, allowDataLoss, nonInteractive, emitSqlPath, planOnly);
    }

    private static CharismaSchema? TryParseSchema(string schemaPath)
    {
        if (!File.Exists(schemaPath)) return null;
        try
        {
            var schemaText = File.ReadAllText(schemaPath);
            RoslynSchemaParser parser = new();
            return parser.Parse(schemaText);
        }
        catch (Exception ex)
        {
            CliConsole.Error($"Warning: could not parse existing schema at {schemaPath}: {ex.Message}");
            return null;
        }
    }

    private static string? TryExtractDatasourceUrl(string schemaPath)
    {
        if (!File.Exists(schemaPath)) return null;
        var text = File.ReadAllText(schemaPath);

        // Capture the first url assignment inside any datasource block.
        var m = Regex.Match(text, @"datasource\s+\w+\s*\{[^}]*?url\s*=\s*(?<url>[^\r\n]+)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!m.Success) return null;

        var url = m.Groups["url"].Value.Trim();

        // env("FOO") or env('FOO') should be returned verbatim so ResolveConnectionString can resolve env expansion.
        if (url.StartsWith("\"", StringComparison.Ordinal) && url.EndsWith("\"", StringComparison.Ordinal))
        {
            url = url.Substring(1, url.Length - 2);
        }
        return url.Length == 0 ? null : url;
    }

    private static string? ResolveConnectionString(string? supplied, CharismaSchema? existingSchema)
    {
        if (!string.IsNullOrEmpty(supplied))
            return supplied;

        // Global env fallback
        var env = Environment.GetEnvironmentVariable("CHARISMA_CONNECTION_STRING")
                  ?? Environment.GetEnvironmentVariable("DATABASE_URL");
        if (!string.IsNullOrWhiteSpace(env))
            return env;

        if (existingSchema is null || existingSchema.Datasources.Count == 0)
        {
            Console.Error.WriteLine("No connection string found. Provide --connection, set CHARISMA_CONNECTION_STRING or DATABASE_URL, or use runtime options in your app.");
            return null;
        }

        var url = existingSchema.Datasources[0].Url.Trim();

        // env("VAR") or env('VAR')
        var envMatch = Regex.Match(url, "^env\\(\\\"?(?<var>[^\\\"]+)\\\"?\\)$", RegexOptions.IgnoreCase);
        if (envMatch.Success)
        {
            var varName = envMatch.Groups["var"].Value;
            var value = Environment.GetEnvironmentVariable(varName);
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }

        // Strip quotes if present
        if (url.StartsWith("\"", StringComparison.Ordinal) && url.EndsWith("\"", StringComparison.Ordinal))
            url = url.Substring(1, url.Length - 2);

        return url;
    }

    private static ClientConfig BuildClientConfig(CharismaSchema schema, string? connectionOverride, string? rootNamespaceOverride, string? outputOverride)
    {
        var gen = SelectGenerator(schema);
        var rootNs = rootNamespaceOverride
                     ?? gen?.Get("rootNamespace")
                     ?? gen?.Get("root-namespace")
                     ?? gen?.Get("namespace")
                     ?? DeriveDefaultRootNamespace();
        var output = outputOverride
                     ?? gen?.Get("output")
                     ?? gen?.Get("out")
                     ?? Path.Combine(Directory.GetCurrentDirectory(), "Generated");

        var connection = ResolveConnectionString(connectionOverride, schema);
        return new ClientConfig(connection, rootNs!, output);
    }

    private static GeneratorDefinition? SelectGenerator(CharismaSchema schema)
    {
        var byName = schema.Generators.FirstOrDefault(g => string.Equals(g.Name, "client", StringComparison.OrdinalIgnoreCase));
        return byName ?? schema.Generators.FirstOrDefault();
    }

    private static string ReplaceDatasourceBlock(string existing, CharismaSchema schema)
    {
        if (schema.Datasources.Count == 0)
        {
            return existing;
        }

        var block = BuildDatasourceBlock(schema.Datasources[0]);
        var regex = new Regex(@"datasource\s+\w+\s*\{[^}]*\}", RegexOptions.Singleline | RegexOptions.Compiled);

        if (regex.IsMatch(existing))
        {
            return regex.Replace(existing, block, 1);
        }

        // No existing datasource: prepend.
        return block + Environment.NewLine + existing;
    }

    private static string BuildDatasourceBlock(DatasourceDefinition datasource)
    {
        var sb = new StringBuilder();
        sb.Append("datasource ");
        sb.Append(datasource.Name);
        sb.AppendLine(" {");
        sb.Append("  provider = \"");
        sb.Append(datasource.Provider);
        sb.AppendLine("\"");
        sb.Append("  url = ");
        sb.AppendLine(datasource.Url);
        foreach (var opt in datasource.Options.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            sb.Append("  ");
            sb.Append(opt.Key);
            sb.Append(" = \"");
            sb.Append(opt.Value);
            sb.AppendLine("\"");
        }
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static void PrintUsage()
    {
        CliConsole.Info("Usage:");
        CliConsole.Info("  charisma [--help|-h]");
        CliConsole.Bullet("prints this help overview");
        CliConsole.Info("  charisma generate [schemaPath] [outputPath] [--root-namespace MyApp.Generated]");
        CliConsole.Bullet("defaults: schema.charisma in cwd, output ./Generated or generator.output, root namespace from generator or <cwd>.Generated");
        CliConsole.Info("  charisma db pull [schemaPath] [--connection <conn>] [--force]");
        CliConsole.Bullet("defaults: schema.charisma in cwd; connection from --connection, CHARISMA_CONNECTION_STRING/DATABASE_URL, or datasource env/url");
        CliConsole.Bullet("--force rewrites file; otherwise only datasource block is updated if present");
        CliConsole.Info("  charisma db push [schemaPath] [--connection <conn>] [--force-reset] [--accept-data-loss] [--yes] [--emit-sql <file>] [--plan-only]");
        CliConsole.Bullet("defaults: schema.charisma in cwd; connection from env/datasource; non-interactive uses --yes; --emit-sql writes SQL; --plan-only previews");
        CliConsole.Info("  charisma migrate ... (not implemented yet)");
    }

    private static int FailUsage()
    {
        PrintUsage();
        return 1;
    }

    /// <summary>
    /// Extracts the --root-namespace argument if provided.
    /// </summary>
    /// <param name="args">Command-line arguments.</param>
    /// <returns>The root namespace value, or null when not supplied.</returns>
    private static string? ParseRootNamespaceArg(string[] args)
    {
        for (int i = 0; i < args.Length - 1; i++)
        {
            if (string.Equals(args[i], "--root-namespace", StringComparison.OrdinalIgnoreCase))
            {
                return args[i + 1];
            }
        }
        return null;
    }

    /// <summary>
    /// Derives a default root namespace based on the current working directory name.
    /// </summary>
    /// <returns>Derived root namespace in the form &lt;cwd&gt;.Generated.</returns>
    private static string DeriveDefaultRootNamespace()
    {
        var dir = new DirectoryInfo(Directory.GetCurrentDirectory()).Name;
        return $"{dir}.Generated";
    }
}