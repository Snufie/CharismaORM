using System;
using System.Collections.Generic;

namespace Charisma.Schema
{
    /// <summary>
    /// Typed relation metadata that the Validator/Generator can populate.
    /// This type models the common pieces needed: local fields, foreign model, foreign fields, cardinality and optional
    /// explicit relation name.
    /// It is intentionally minimal — the parser is not required to populate it, but the Validator/Generator may do so.
    /// </summary>
    public sealed class RelationInfo
    {
        /// <summary>
        /// Optional explicit relation name (Prisma allows naming relations).
        /// </summary>
        public string? RelationName { get; }

        /// <summary>
        /// The referenced model name (foreign model).
        /// </summary>
        public string ForeignModel { get; }

        /// <summary>
        /// Local field(s) forming the FK (may be single or composite).
        /// </summary>
        public IReadOnlyList<string> LocalFields { get; }

        /// <summary>
        /// Foreign field(s) forming the referenced key (primary or unique) in the foreign model.
        /// </summary>
        public IReadOnlyList<string> ForeignFields { get; }

        /// <summary>
        /// True if this side is a collection (many), false if single (one).
        /// </summary>
        public bool IsCollection { get; }

        /// <summary>
        /// Behavior to apply when the referenced record is deleted.
        /// Defaults to SetNull to align with Prisma semantics when unspecified.
        /// </summary>
        public OnDeleteBehavior OnDelete { get; }
        public OnDeleteBehavior? OnUpdate { get; }

        public RelationInfo(
            string foreignModel,
            IReadOnlyList<string> localFields,
            IReadOnlyList<string> foreignFields,
            bool isCollection,
            string? relationName = null,
               OnDeleteBehavior onDelete = OnDeleteBehavior.SetNull,
               OnDeleteBehavior? onUpdate = null)
        {
            ForeignModel = foreignModel ?? throw new ArgumentNullException(nameof(foreignModel));
            LocalFields = localFields ?? throw new ArgumentNullException(nameof(localFields));
            ForeignFields = foreignFields ?? throw new ArgumentNullException(nameof(foreignFields));
            IsCollection = isCollection;
            RelationName = relationName;
            OnDelete = onDelete;
            OnUpdate = onUpdate;
        }
    }
}
