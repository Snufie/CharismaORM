using System.Collections.Generic;
using Charisma.Schema;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Charisma.Generator;

/// <summary>
/// Entry-point contract for Phase 1 code generation.
/// </summary>
public interface ICharismaGenerator
{
    /// <summary>
    /// Executes Phase 1 generation for a validated schema.
    /// </summary>
    /// <param name="schema">Validated, normalized schema from Phase 0.</param>
    /// <returns>All generated compilation units.</returns>
    IReadOnlyList<CompilationUnitSyntax> Generate(CharismaSchema schema);
}
