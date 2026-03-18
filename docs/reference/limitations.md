# Limitations

This page tracks known current limitations in the implementation.

## Provider Scope

- PostgreSQL path is the implemented provider flow.
- Other DB provider paths are not currently implemented.

## CLI Scope

- Running `charisma` with no arguments prints usage/help (no-op).
- `charisma migrate ...` command family is not implemented yet.

## Query Surface Constraints

- `Select`, `Include`, and `Omit` are mutually exclusive in a single args object.
- `Include` is currently rejected in delete and bulk mutation executor paths.
- Nested relation directives are supported selectively; unsupported directives throw runtime `NotSupportedException`.
- Some planner features enforce strict scalar/field constraints (for example in distinct/groupBy selectors).
- Nesting depth limits apply for include/select graph traversal.

## Migration Scope Constraints

- Migration behavior is tuned to PostgreSQL-specific SQL semantics.
- Some advanced DB features are outside current first-class DSL/planner modeling scope.

## Schema Validation Constraints

Certain syntactic or semantic combinations are intentionally rejected for safety and determinism:

- incompatible default/type pairings
- malformed relation endpoint syntax
- conflicting key definitions

## Operational Caveats

- `--force-reset` should be treated as a destructive development workflow.
- Always review warnings and plan output before applying destructive changes.

## Recommendation

Use this sequence to reduce surprises:

1. validate schema by running generation
2. preview DB changes with `db push --plan-only`
3. apply with explicit data-loss acceptance only when intended
