using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    public enum DefaultValueKind
    {
        Static,
        Autoincrement,
        UuidV4,
        UuidV7,
        Now,
        Json
    }

    public sealed record DefaultValueDefinition(DefaultValueKind Kind, string? Value = null);

    /// <summary>
    /// Base class for field definitions.
    /// Attributes are preserved as raw strings (e.g., "@id", "@default(autoincrement())").
    /// </summary>
    public abstract class FieldDefinition
    {
        public string Name { get; }
        /// <summary>Raw type token as declared (e.g., "String", "User"). Does not include list or optional markers.</summary>
        public string RawType { get; }
        public bool IsList { get; }
        public bool IsOptional { get; }
        /// <summary>Field-level attributes (excluding @relation contents) represented as raw tokens (e.g., "@id", "@unique").</summary>
        public IReadOnlyList<string> Attributes { get; }

        protected FieldDefinition(
            string name,
            string rawType,
            bool isList,
            bool isOptional,
            IReadOnlyList<string> attributes)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            RawType = rawType ?? throw new ArgumentNullException(nameof(rawType));
            IsList = isList;
            IsOptional = isOptional;
            Attributes = attributes ?? throw new ArgumentNullException(nameof(attributes));
        }
    }

    /// <summary>
    /// Scalar field (primitive type) definition.
    /// </summary>
    public sealed class ScalarFieldDefinition : FieldDefinition
    {
        public bool IsId { get; }
        public bool IsUnique { get; }
        public bool IsUpdatedAt { get; }
        public DefaultValueDefinition? DefaultValue { get; }

        public ScalarFieldDefinition(
            string name,
            string rawType,
            bool isList,
            bool isOptional,
            IReadOnlyList<string> attributes,
            bool isId = false,
            bool isUnique = false,
            bool isUpdatedAt = false,
            DefaultValueDefinition? defaultValue = null)
            : base(name, rawType, isList, isOptional, attributes)
        {
            IsId = isId;
            IsUnique = isUnique;
            IsUpdatedAt = isUpdatedAt;
            DefaultValue = defaultValue;
        }
    }

    /// <summary>
    /// Relation field definition.
    /// This includes raw relation attribute tokens plus an optional typed RelationInfo that may be filled by a validation/resolution step.
    /// </summary>
    public sealed class RelationFieldDefinition : FieldDefinition
    {
        /// <summary>
        /// Raw args captured from @relation(...). Each entry typically corresponds to a top-level argument inside the relation call,
        /// e.g. "fk(User.id)" or "fk(User.id), pk(Post.authorId)" depending on how the parser captured tokens.
        /// </summary>
        public IReadOnlyList<string> RelationAttributes { get; }

        /// <summary>
        /// Once the parser and/or validator resolves the relation, RelationInfo may be filled with the typed pieces.
        /// This is optional to allow parser-first, validate-later workflows.
        /// </summary>
        public RelationInfo? RelationInfo { get; }

        public RelationFieldDefinition(
            string name,
            string rawType,
            bool isList,
            bool isOptional,
            IReadOnlyList<string> attributes,
            IReadOnlyList<string> relationAttributes,
            RelationInfo? relationInfo = null)
            : base(name, rawType, isList, isOptional, attributes)
        {
            RelationAttributes = relationAttributes ?? throw new ArgumentNullException(nameof(relationAttributes));
            RelationInfo = relationInfo;
        }
    }
}
