using System;
using System.IO;
using System.Linq;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Charisma.Generator;

/// <summary>
/// Centralized deterministic filename derivation logic.
/// Writers never participate in path decisions.
/// </summary>
internal static class FileNameDerivation
{
    /// <summary>
    /// Determines the output path for a generated compilation unit.
    /// </summary>
    /// <param name="rootOutputDirectory">Root Generated output folder.</param>
    /// <param name="unit">Compilation unit being routed.</param>
    /// <returns>Absolute path where the generated file should be written.</returns>
    public static string DerivePath(
        string rootOutputDirectory,
        CompilationUnitSyntax unit)
    {
        var ns = unit.Members[0] switch
        {
            NamespaceDeclarationSyntax n => n.Name.ToString(),
            _ => throw new InvalidOperationException(
                "Top-level namespace declaration expected.")
        };

        var typeName = ExtractPrimaryTypeName(unit);

        return ns switch
        {
            var n when n.EndsWith(".Enums") =>
                Path.Combine(rootOutputDirectory, "Enums", $"{typeName}.g.cs"),

            var n when n.EndsWith(".Models") && typeName.EndsWith("Delegate") =>
                Path.Combine(rootOutputDirectory, "Delegates", $"{typeName}.g.cs"),

            var n when n.EndsWith(".Models") =>
                Path.Combine(rootOutputDirectory, "Models", $"{typeName}.g.cs"),

            var n when n.EndsWith(".Args") =>
                Path.Combine(rootOutputDirectory, "Args", $"{DeriveArgsFileName(unit)}.g.cs"),

            var n when n.EndsWith(".Filters") =>
                Path.Combine(rootOutputDirectory, "Filters", $"{DeriveFilterFileName(unit)}.g.cs"),

            var n when n.EndsWith(".Select") =>
                Path.Combine(rootOutputDirectory, "Select", $"{typeName}.g.cs"),

            var n when n.EndsWith(".Omit") =>
                Path.Combine(rootOutputDirectory, "Omit", $"{typeName}.g.cs"),

            var n when n.EndsWith(".Include") =>
                Path.Combine(rootOutputDirectory, "Include", $"{typeName}.g.cs"),

            var n when n.EndsWith(".Metadata") =>
                Path.Combine(rootOutputDirectory, "Metadata", $"{typeName}.g.cs"),

            // CharismaClient at the root namespace
            _ when typeName == "CharismaClient" =>
                Path.Combine(rootOutputDirectory, "CharismaClient.g.cs"),

            _ => throw new InvalidOperationException(
                $"Unsupported namespace for output routing: {ns}")
        };
    }
    /// <summary>
    /// Extracts the primary public type name from a compilation unit.
    /// </summary>
    /// <param name="unit">Compilation unit to inspect.</param>
    /// <returns>Name of the first public type/enum.</returns>
    private static string ExtractPrimaryTypeName(CompilationUnitSyntax unit)
    {
        foreach (var member in unit.DescendantNodes())
        {
            switch (member)
            {
                case TypeDeclarationSyntax type when type.Modifiers.Any(SyntaxKind.PublicKeyword):
                    return type.Identifier.Text;
                case EnumDeclarationSyntax en when en.Modifiers.Any(SyntaxKind.PublicKeyword):
                    return en.Identifier.Text;
            }
        }

        throw new InvalidOperationException("No public type declaration found.");
    }

    /// <summary>
    /// Collapses specific args/input type suffixes to a stable args filename stem.
    /// </summary>
    /// <param name="unit">Compilation unit containing arg/input types.</param>
    /// <returns>File name stem (without extension) for args outputs.</returns>
    private static string DeriveArgsFileName(CompilationUnitSyntax unit)
    {
        var primary = ExtractPrimaryTypeName(unit);
        var suffixes = new[]
        {
            "FindUniqueArgs",
            "FindFirstArgs",
            "FindManyArgs",
            "CountArgs",
            "CreateArgs",
            "CreateManyArgs",
            "UpdateArgs",
            "UpdateManyArgs",
            "DeleteArgs",
            "DeleteManyArgs",
            "UpsertArgs",
            "CreateInput",
            "UpdateInput",
            "UpsertInput",
            "CreateOrConnectInput",
            "UpsertNestedInput",
            "UpdateWithWhereInput",
            "UpdateManyWithWhereInput",
            "CreateRelationInput",
            "CreateManyRelationInput",
            "UpdateRelationInput",
            "UpdateManyRelationInput",
            "OrderByInput"
        };

        foreach (var suffix in suffixes)
        {
            if (primary.EndsWith(suffix, StringComparison.Ordinal))
            {
                return $"{primary[..^suffix.Length]}Args";
            }
        }

        return primary;
    }

    /// <summary>
    /// Derives filter file names to align with architecture expectations.
    /// </summary>
    /// <param name="unit">Compilation unit containing filter-related types.</param>
    /// <returns>Deterministic file name stem for filter output.</returns>
    private static string DeriveFilterFileName(CompilationUnitSyntax unit)
    {
        var publicTypes = unit.DescendantNodes()
            .Where(n => n is TypeDeclarationSyntax or EnumDeclarationSyntax)
            .Select(n => n switch
            {
                TypeDeclarationSyntax t when t.Modifiers.Any(SyntaxKind.PublicKeyword) => t.Identifier.Text,
                EnumDeclarationSyntax e when e.Modifiers.Any(SyntaxKind.PublicKeyword) => e.Identifier.Text,
                _ => null
            })
            .Where(n => n is not null)
            .Select(n => n!)
            .ToList();

        // Prefer the canonical scalar filter hub when present.
        if (publicTypes.Contains("StringFilter", StringComparer.Ordinal))
        {
            return "StringFilter";
        }

        var whereUnique = publicTypes.FirstOrDefault(n => n.EndsWith("WhereUniqueInput", StringComparison.Ordinal));
        if (!string.IsNullOrWhiteSpace(whereUnique))
        {
            return whereUnique![..^"WhereUniqueInput".Length] + "Filter";
        }

        var firstFilter = publicTypes.FirstOrDefault(n => n.EndsWith("Filter", StringComparison.Ordinal));
        if (!string.IsNullOrWhiteSpace(firstFilter))
        {
            return firstFilter!;
        }

        var whereLike = publicTypes.FirstOrDefault(n => n.EndsWith("WhereInput", StringComparison.Ordinal));
        if (!string.IsNullOrWhiteSpace(whereLike))
        {
            return whereLike![..^"WhereInput".Length] + "Filter";
        }

        return ExtractPrimaryTypeName(unit);
    }
}