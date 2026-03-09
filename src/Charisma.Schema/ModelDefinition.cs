using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    /// <summary>
    /// Represents a model declaration.
    /// Fields are ordered as declared.
    /// Model-level attributes (e.g., @@unique, @@index) are preserved as raw strings and also represented as typed constraints.
    /// </summary>
    public sealed class ModelDefinition
    {
        public string Name { get; }
        public IReadOnlyList<FieldDefinition> Fields { get; }
        public IReadOnlyList<string> Attributes { get; }

        /// <summary>
        /// Optional primary key (single or composite).
        /// </summary>
        public PrimaryKeyDefinition? PrimaryKey { get; }

        /// <summary>
        /// Unique constraints declared on the model (includes single-field unique constraints and @@unique).
        /// </summary>
        public IReadOnlyList<UniqueConstraintDefinition> UniqueConstraints { get; }

        /// <summary>
        /// Index declarations (includes @@index blocks).
        /// </summary>
        public IReadOnlyList<IndexDefinition> Indexes { get; }

        public ModelDefinition(
            string name,
            IReadOnlyList<FieldDefinition> fields,
            IReadOnlyList<string> attributes,
            PrimaryKeyDefinition? primaryKey = null,
            IReadOnlyList<UniqueConstraintDefinition>? uniqueConstraints = null,
            IReadOnlyList<IndexDefinition>? indexes = null)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Fields = fields ?? throw new ArgumentNullException(nameof(fields));
            Attributes = attributes ?? throw new ArgumentNullException(nameof(attributes));
            PrimaryKey = primaryKey;
            UniqueConstraints = uniqueConstraints ?? Array.Empty<UniqueConstraintDefinition>();
            Indexes = indexes ?? Array.Empty<IndexDefinition>();
        }

        /// <summary>
        /// Convenience: find a field by name (case-sensitive).
        /// </summary>
        public FieldDefinition? GetField(string fieldName)
        {
            for (int i = 0; i < Fields.Count; i++)
            {
                if (Fields[i].Name == fieldName) return Fields[i];
            }
            return null;
        }
    }
}
