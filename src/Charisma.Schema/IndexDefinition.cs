using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    /// <summary>
    /// Represents an index declared on a model (@@index or similar).
    /// </summary>
    public sealed class IndexDefinition
    {
        public IReadOnlyList<string> Fields { get; }
        public bool IsUnique { get; }
        public string? Name { get; }

        public IndexDefinition(IReadOnlyList<string> fields, bool isUnique = false, string? name = null)
        {
            Fields = fields ?? throw new ArgumentNullException(nameof(fields));
            IsUnique = isUnique;
            Name = name;
        }
    }
}
