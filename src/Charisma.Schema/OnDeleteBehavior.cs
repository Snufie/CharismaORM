namespace Charisma.Schema
{
    /// <summary>
    /// Describes how a relation behaves when the foreign key target is deleted.
    /// Mirrors Prisma's onDelete semantics.
    /// </summary>
    public enum OnDeleteBehavior
    {
        Cascade,
        SetNull,
        Restrict,
        NoAction
    }
}
