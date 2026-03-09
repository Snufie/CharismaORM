using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    /// <summary>
    /// Represents a unique constraint (single-field unique or composite) declared on a model.
    /// </summary>
    public sealed class UniqueConstraintDefinition
    {
        public IReadOnlyList<string> Fields { get; }
        public string? Name { get; }

        public UniqueConstraintDefinition(IReadOnlyList<string> fields, string? name = null)
        {
            Fields = fields ?? throw new ArgumentNullException(nameof(fields));
            Name = name;
        }
    }
}
