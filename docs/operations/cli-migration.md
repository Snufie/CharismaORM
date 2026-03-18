# Charisma CLI & Migration

## CLI Commands

- `charisma generate [schemaPath] [outputPath] [--root-namespace <ns>]`
- `charisma db pull [schemaPath] [--connection <conn>] [--force]`
- `charisma db push [schemaPath] [--connection <conn>] [--force-reset] [--accept-data-loss] [--yes] [--emit-sql <file>] [--plan-only]`
- `charisma migrate [schemaPath] [--connection <conn>]`

## Migration Information

- Migration planning and execution is supported via CLI and runtime.
- Use `db push` for schema sync and destructive changes.
- Use `migrate` for safe migration planning and application.
- Migration runner supports preview, apply, and reset modes.

## Usage Example

```bash
charisma migrate schema.charisma --connection "Host=localhost;Database=mydb;Username=postgres;Password=postgres"
```

## Tips

- Always preview migration plan before applying.
- Use `--force-reset` for development resets.
- Migration safety checks prevent data loss unless overridden.
