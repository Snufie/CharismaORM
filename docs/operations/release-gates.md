# Release Gates

This page defines explicit release gates for CharismaORM and how they map to CI jobs.

## Gate Policy

A release is allowed only when all gates below are green.

1. Build gate

- All source and test projects build in `Release`.
- `tests/TestApp` is intentionally excluded from release gates because it is a local integration harness that depends on generated namespaces.

1. Unit and component test gate

- All `tests/Charisma.*.Tests` suites pass.
- No failed tests are allowed.

1. PostgreSQL integration gate

- Migration integration tests run with `CHARISMA_RUN_PG_INTEGRATION=1`.
- Integration tests must not be skipped in CI due to missing infrastructure.

1. Dependency security gate

- `dotnet list Charisma.sln package --vulnerable --include-transitive` reports no known vulnerabilities.

## CI Mapping

Workflow: `.github/workflows/ci-release-gates.yml`

Jobs:

- `build-and-unit` -> gates 1 and 2
- `postgres-integration` -> gate 3
- `dependency-audit` -> gate 4
- `release-gates` -> aggregate pass/fail status

## Local Verification Commands

Use these before tagging a release:

```bash
dotnet build src/Charisma.All/Charisma.All.csproj -c Release
dotnet build tests/Charisma.Generator.Tests/Charisma.Generator.Tests.csproj -c Release
dotnet test tests/Charisma.Schema.Tests/Charisma.Schema.Tests.csproj -c Release
dotnet test tests/Charisma.Parser.Tests/Charisma.Parser.Tests.csproj -c Release
dotnet test tests/Charisma.QueryEngine.Tests/Charisma.QueryEngine.Tests.csproj -c Release
dotnet test tests/Charisma.Generator.Tests/Charisma.Generator.Tests.csproj -c Release
dotnet test tests/Charisma.Migration.Tests/Charisma.Migration.Tests.csproj -c Release
dotnet list Charisma.sln package --vulnerable --include-transitive
```

To run PostgreSQL integration tests locally:

```bash
CHARISMA_RUN_PG_INTEGRATION=1 dotnet test tests/Charisma.Migration.Tests/Charisma.Migration.Tests.csproj -c Release
```

## Why TestApp Is Excluded

`tests/TestApp` is not a deterministic gate target for release because it expects generated source namespace wiring that is not part of the default repository build path.

If you want to include it later, first add a deterministic prebuild generation step and explicit project references to generated output.
