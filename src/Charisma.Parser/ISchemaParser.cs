using Charisma.Schema;

namespace Charisma.Parser
{
    /// <summary>
    /// Parse the schema.charisma content and return a CharismaSchema IR.
    /// Throws CharismaSchemaAggregateException on error with diagnostics.
    /// </summary>
    public interface ISchemaParser
    {
        /// <summary>
        /// Parses and validates schema text into the in-memory schema model.
        /// </summary>
        /// <param name="schemaText">Raw DSL contents of schema.charisma.</param>
        /// <returns>Validated schema or throws on errors.</returns>
        CharismaSchema Parse(string schemaText);
    }
}
