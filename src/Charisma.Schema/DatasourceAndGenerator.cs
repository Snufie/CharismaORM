using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    /// <summary>
    /// Datasource block representation (datasource db { provider = "postgresql", url = env("DATABASE_URL") })
    /// </summary>
    public sealed class DatasourceDefinition
    {
        public string Name { get; }
        public string Provider { get; }
        /// <summary>
        /// The raw url expression (e.g. env("DATABASE_URL") or a literal). Parser preserves raw form.
        /// </summary>
        public string Url { get; }

        public IReadOnlyDictionary<string, string> Options { get; }

        public DatasourceDefinition(string name, string provider, string url, IDictionary<string, string>? options = null)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Url = url ?? throw new ArgumentNullException(nameof(url));
            Options = new Dictionary<string, string>(options ?? new Dictionary<string, string>(), StringComparer.Ordinal);
        }
    }

    /// <summary>
    /// Generator block representation (generator client { provider = \"charisma-generator\", output = \"./Generated\" })
    /// </summary>
    public sealed class GeneratorDefinition
    {
        public string Name { get; }
        public IReadOnlyDictionary<string, string> Config { get; }

        public GeneratorDefinition(string name, IDictionary<string, string>? config = null)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Config = new Dictionary<string, string>(config ?? new Dictionary<string, string>(), StringComparer.Ordinal);
        }

        /// <summary>
        /// Helper to get configuration by key with an optional default.
        /// </summary>
        public string? Get(string key, string? defaultValue = null)
        {
            if (Config.TryGetValue(key, out var v)) return v;
            return defaultValue;
        }
    }
}
