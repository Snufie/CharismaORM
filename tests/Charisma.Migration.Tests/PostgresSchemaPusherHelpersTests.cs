using Charisma.Migration.Introspection.Push.Postgres;
using Charisma.Schema;

namespace Charisma.Migration.Tests;

public sealed class PostgresSchemaPusherHelpersTests
{
    [Fact]
    public void QuoteIdentifier_EscapesEmbeddedDoubleQuotes()
    {
        var quoted = PostgresSchemaPusherHelpers.QuoteIdentifier("a\"b");
        Assert.Equal("\"a\"\"b\"", quoted);
    }

    [Fact]
    public void QuoteIdentifier_NullOrWhitespace_Throws()
    {
        Assert.Throws<ArgumentException>(() => PostgresSchemaPusherHelpers.QuoteIdentifier(""));
        Assert.Throws<ArgumentException>(() => PostgresSchemaPusherHelpers.QuoteIdentifier("   "));
    }

    [Fact]
    public void BuildEnum_EscapesEnumValues()
    {
        var enumDef = new EnumDefinition("RobotState", new[] { "OK", "O'HARE" });
        var sql = PostgresSchemaPusherHelpers.BuildEnum(enumDef);

        Assert.Contains("'O''HARE'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("'O'HARE'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildDropInboundForeignKeys_EscapesTableAndColumnLiterals()
    {
        var sql = PostgresSchemaPusherHelpers.BuildDropInboundForeignKeys("Ta'ble", "Co'l");

        Assert.Contains("ccu.table_name = 'Ta''ble'", sql, StringComparison.Ordinal);
        Assert.Contains("ccu.column_name = 'Co''l'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildDropTable_EscapesIdentifier()
    {
        var sql = PostgresSchemaPusherHelpers.BuildDropTable("my\"table");
        Assert.Contains("drop table if exists \"my\"\"table\" cascade;", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildColumn_UsesQuotedIdentifier_AndEscapesStaticDefault()
    {
        var field = new ScalarFieldDefinition(
            name: "na\"me",
            rawType: "String",
            isList: false,
            isOptional: false,
            attributes: Array.Empty<string>(),
            defaultValue: new DefaultValueDefinition(DefaultValueKind.Static, "O'Hare"));

        var sql = PostgresSchemaPusherHelpers.BuildColumn(field, isEnum: false);
        Assert.Contains("\"na\"\"me\" text default 'O''Hare' not null", sql, StringComparison.Ordinal);
    }
}
