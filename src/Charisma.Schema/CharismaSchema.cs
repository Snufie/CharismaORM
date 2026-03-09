
namespace Charisma.Schema
{
    /// <summary>
    /// Root IR object representing the parsed schema.
    /// Immutable container for Models, Enums, Datasources and Generators.
    /// </summary>
    public sealed class CharismaSchema
    {
        public IReadOnlyDictionary<string, ModelDefinition> Models { get; }
        public IReadOnlyDictionary<string, EnumDefinition> Enums { get; }
        public IReadOnlyList<DatasourceDefinition> Datasources { get; }
        public IReadOnlyList<GeneratorDefinition> Generators { get; }

        /// <summary>
        /// Canonical, deterministic textual representation of the schema.
        /// Used for hashing and generator determinism.
        /// </summary>
        public string CanonicalText { get; }

        /// <summary>
        /// Precomputed schema hash based on <see cref="CanonicalText"/>.
        /// </summary>
        public string SchemaHash { get; }

        public CharismaSchema(
            IDictionary<string, ModelDefinition> models,
            IDictionary<string, EnumDefinition> enums,
            IList<DatasourceDefinition> datasources,
            IList<GeneratorDefinition> generators)
        {
            ArgumentNullException.ThrowIfNull(models);
            ArgumentNullException.ThrowIfNull(enums);
            ArgumentNullException.ThrowIfNull(datasources);
            ArgumentNullException.ThrowIfNull(generators);

            Models = new Dictionary<string, ModelDefinition>(models, StringComparer.Ordinal);
            Enums = new Dictionary<string, EnumDefinition>(enums, StringComparer.Ordinal);
            Datasources = new List<DatasourceDefinition>(datasources).AsReadOnly();
            Generators = new List<GeneratorDefinition>(generators).AsReadOnly();

            CanonicalText = SchemaNormalizer.Normalize(this);
            SchemaHash = SchemaHasher.Compute(CanonicalText);
            Console.WriteLine($"Models: {Models.Count}, Enums: {Enums.Count}, Datasources: {Datasources.Count}, Generators: {Generators.Count}");
        }

        public bool TryGetModel(string name, out ModelDefinition? model) =>
            Models.TryGetValue(name, out model);
    }
}
