using System.Reflection;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator.Tests;

public sealed class FileNameDerivationTests
{
    [Fact]
    public void DerivePath_WithoutTopLevelNamespace_Throws()
    {
        var unit = Parse("public class A {} ");
        var ex = Assert.Throws<TargetInvocationException>(() => InvokeDerivePath("C:\\out", unit));
        Assert.Contains("Top-level namespace declaration expected", ex.InnerException?.Message);
    }

    [Fact]
    public void DerivePath_UnsupportedNamespace_Throws()
    {
        var unit = Parse("namespace Charisma.Generated.Unknown { public class A {} }");
        var ex = Assert.Throws<TargetInvocationException>(() => InvokeDerivePath("C:\\out", unit));
        Assert.Contains("Unsupported namespace for output routing", ex.InnerException?.Message);
    }

    [Fact]
    public void DerivePath_NoPublicType_Throws()
    {
        var unit = Parse("namespace Charisma.Generated.Models { internal class A {} }");
        var ex = Assert.Throws<TargetInvocationException>(() => InvokeDerivePath("C:\\out", unit));
        Assert.Contains("No public type declaration found", ex.InnerException?.Message);
    }

    [Fact]
    public void DerivePath_ModelsDelegate_GoesToDelegatesFolder()
    {
        var unit = Parse("namespace Charisma.Generated.Models { public class RobotDelegate {} }");
        var path = InvokeDerivePath("C:\\out", unit);
        Assert.EndsWith(Path.Combine("Delegates", "RobotDelegate.g.cs"), path, StringComparison.Ordinal);
    }

    [Fact]
    public void DerivePath_ArgsSuffixes_CollapseToStableArgsStem()
    {
        var unit = Parse("namespace Charisma.Generated.Args { public class RobotFindManyArgs {} }");
        var path = InvokeDerivePath("C:\\out", unit);
        Assert.EndsWith(Path.Combine("Args", "RobotArgs.g.cs"), path, StringComparison.Ordinal);
    }

    [Fact]
    public void DerivePath_Filters_PrefersStringFilterHubWhenPresent()
    {
        var unit = Parse("namespace Charisma.Generated.Filters { public class StringFilter {} public class RobotFilter {} }");
        var path = InvokeDerivePath("C:\\out", unit);
        Assert.EndsWith(Path.Combine("Filters", "StringFilter.g.cs"), path, StringComparison.Ordinal);
    }

    [Fact]
    public void DerivePath_Filters_WhereUniqueInput_BecomesModelFilterFile()
    {
        var unit = Parse("namespace Charisma.Generated.Filters { public class RobotWhereUniqueInput {} }");
        var path = InvokeDerivePath("C:\\out", unit);
        Assert.EndsWith(Path.Combine("Filters", "RobotFilter.g.cs"), path, StringComparison.Ordinal);
    }

    [Fact]
    public void DerivePath_RootClient_MapsToCharismaClientFile()
    {
        var unit = Parse("namespace Charisma.Generated { public class CharismaClient {} }");
        var path = InvokeDerivePath("C:\\out", unit);
        Assert.EndsWith("CharismaClient.g.cs", path, StringComparison.Ordinal);
    }

    private static CompilationUnitSyntax Parse(string code)
    {
        return CSharpSyntaxTree.ParseText(code).GetCompilationUnitRoot();
    }

    private static string InvokeDerivePath(string root, CompilationUnitSyntax unit)
    {
        var type = typeof(CharismaGenerator).Assembly.GetType("Charisma.Generator.FileNameDerivation");
        Assert.NotNull(type);

        var method = type!.GetMethod("DerivePath", BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(method);

        return (string)method!.Invoke(null, new object[] { root, unit })!;
    }
}
