using System.Collections.Generic;
using Charisma.Schema;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator;

/// <summary>
/// Contract implemented by all Phase 1 writers.
/// Writers emit syntax only and remain IO-agnostic.
/// </summary>
public interface IWriter
{

    /// <summary>
    /// Emits zero or more compilation units.
    /// </summary>
    IReadOnlyList<CompilationUnitSyntax> Write(CharismaSchema schema);
}
