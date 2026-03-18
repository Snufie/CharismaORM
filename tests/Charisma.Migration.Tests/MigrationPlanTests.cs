namespace Charisma.Migration.Tests;

public sealed class MigrationPlanTests
{
    [Fact]
    public void Flags_ReflectPlanContents()
    {
        var plan = new MigrationPlan(
            Steps: new[] { new MigrationStep("drop", IsDestructive: true, Sql: "drop table x;") },
            Warnings: new[] { "warn" },
            Unexecutable: new[] { "blocked" });

        Assert.True(plan.HasDestructiveChanges);
        Assert.True(plan.HasWarnings);
        Assert.True(plan.HasUnexecutable);
    }

    [Fact]
    public void EmptyPlan_HasNoFlags()
    {
        Assert.False(MigrationPlan.Empty.HasDestructiveChanges);
        Assert.False(MigrationPlan.Empty.HasWarnings);
        Assert.False(MigrationPlan.Empty.HasUnexecutable);
    }
}
