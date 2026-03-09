using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    /// <summary>
    /// Enum definition with ordered values as declared.
    /// </summary>
    public sealed class EnumDefinition
    {
        public string Name { get; }
        public IReadOnlyList<string> Values { get; }

        public EnumDefinition(string name, IReadOnlyList<string> values)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Values = values ?? throw new ArgumentNullException(nameof(values));
        }
    }
}
