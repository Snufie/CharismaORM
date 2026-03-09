using System;
using Microsoft.CodeAnalysis.Text;

namespace Charisma.Parser
{
    /// <summary>
    /// A single parsing/validation diagnostic with optional span.
    /// </summary>
    public sealed class CharismaSchemaException : Exception
    {
        public TextSpan? Span { get; }

        /// <summary>
        /// Creates a schema exception without source span context.
        /// </summary>
        /// <param name="message">Human-readable diagnostic message.</param>
        public CharismaSchemaException(string message)
            : base(message)
        {
            Span = null;
        }

        /// <summary>
        /// Creates a schema exception with an associated source span.
        /// </summary>
        /// <param name="message">Human-readable diagnostic message.</param>
        /// <param name="span">Source span highlighting the error location.</param>
        public CharismaSchemaException(string message, TextSpan span)
            : base(message)
        {
            Span = span;
        }
    }
}
