using System;
using System.Collections.Generic;
using System.Linq;

namespace Charisma.Parser
{
    /// <summary>
    /// Represents an aggregate of parsing/validation errors.
    /// </summary>
    public sealed class CharismaSchemaAggregateException : Exception
    {
        public IReadOnlyList<CharismaSchemaException> Errors { get; }

        /// <summary>
        /// Aggregates multiple schema errors into a single exception with a summary message.
        /// </summary>
        /// <param name="errors">Collection of individual schema exceptions.</param>
        public CharismaSchemaAggregateException(IEnumerable<CharismaSchemaException> errors)
            : base(CreateMessage(errors))
        {
            Errors = errors?.ToList().AsReadOnly() ?? throw new ArgumentNullException(nameof(errors));
        }

        /// <summary>
        /// Builds a concise summary message enumerating all error messages.
        /// </summary>
        /// <param name="errors">Collection of schema exceptions.</param>
        /// <returns>Aggregated message string.</returns>
        private static string CreateMessage(IEnumerable<CharismaSchemaException> errors)
        {
            var arr = errors.Select(e => e.Message).ToArray();
            return $"Schema contains {arr.Length} error(s): {string.Join("; ", arr)}";
        }
    }
}
