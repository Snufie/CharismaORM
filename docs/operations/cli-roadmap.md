# CLI Completion Roadmap

This roadmap defines how to complete the remaining CLI surface so `charisma` behaves like a full production operator tool.

## Current Baseline

Implemented now:

- `charisma` -> help/usage output
- `charisma generate`
- `charisma db pull`
- `charisma db push`

Not implemented yet:

- `charisma migrate ...`

## Command UX Target

Top-level commands:

- `charisma` (help)
- `charisma generate`
- `charisma db pull`
- `charisma db push`
- `charisma migrate status`
- `charisma migrate diff`
- `charisma migrate apply`
- `charisma migrate reset`

Shared flags and behavior:

- `--schema <path>` support for all migration commands
- consistent `--connection <conn>` override
- machine-readable output option (`--json`) for CI scripting
- deterministic non-interactive behavior (`--yes` / explicit refusal when missing)
- standardized exit codes (`0` success, `1` failure, `2` usage)

## Delivery Plan

1. Foundation Pass

- Add a command router object so command parsing is centralized and testable.
- Introduce shared option parsing helpers (`schema`, `connection`, `yes`, `json`).
- Add CLI command tests for help text, unknown commands, and argument errors.

1. `migrate status`

- Show whether DB and schema are in sync.
- Display plan summary counts: steps, destructive, warnings, unexecutable.
- Exit non-zero if unexecutable differences are present.

1. `migrate diff`

- Produce migration plan without applying.
- Add `--emit-sql <file>` and `--plan-only` style output.
- Add `--json` payload for CI consumers.

1. `migrate apply`

- Apply planned steps through `PostgresMigrationRunner`.
- Respect warning/data-loss controls (`--accept-data-loss`, `--yes`).
- Persist execution summary for audit logs.

1. `migrate reset`

- Wrap `PostgresDatabaseResetter` + push flow with explicit destructive confirmation.
- Require `--yes` in non-interactive mode.
- Print strong warning banner before execution.

1. Hardening and parity

- Unify output formatting and error model across `db` and `migrate` families.
- Add end-to-end command tests using ephemeral PostgreSQL containers.
- Update docs and examples for each command path.

## Suggested Test Matrix

- Unit tests for parser/dispatcher by command + flag combinations.
- Integration tests for each command against PostgreSQL testcontainer.
- Negative tests for missing args, conflicting flags, and data-loss refusal paths.
- Snapshot tests for help text to prevent accidental CLI drift.

## Release Readiness Criteria for CLI

- Every documented command has at least one integration test.
- `charisma --help` and `charisma` output are stable and documented.
- Non-interactive command paths are deterministic and CI-safe.
- Error messages include clear next steps and actionable flags.
