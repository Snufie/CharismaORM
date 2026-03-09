using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    /// <summary>
    /// Represents a primary key definition (single or composite) for a model.
    /// </summary>
    public sealed class PrimaryKeyDefinition
    {
        /// <summary>
        /// Ordered list of fields that form the primary key.
        /// </summary>
        public IReadOnlyList<string> Fields { get; }

        /// <summary>
        /// Optional mapped name for the key (e.g., @@id([a, b], name: "PK_Name"))
        /// </summary>
        public string? Name { get; }

        public PrimaryKeyDefinition(IReadOnlyList<string> fields, string? name = null)
        {
            Fields = fields ?? throw new ArgumentNullException(nameof(fields));
            Name = name;
        }
    }
}
