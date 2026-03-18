# Charisma CLI — migrate

The `charisma migrate` command plans and executes migrations against your database using the same planner/runner used by the runtime.

Usage

- Plan only (show SQL):

  charisma migrate --plan-only --emit-sql

- Run migrations (safe):

  charisma migrate --connection "postgres://user:pass@host:5432/dbname"

Options

- `--connection <conn>` — connection string to override the datasource in `schema.charisma`.
- `--plan-only` — do not execute; only print the migration plan and SQL.
- `--emit-sql` — print the generated SQL for the plan.
- `--force-reset` — drop and recreate the schema/database (dangerous).
- `--accept-data-loss` — accept destructive changes during automated migration.

Examples

- Plan only using datasource in working schema:

  charisma migrate --plan-only --emit-sql

- Apply migrations non-interactively using a connection string from env or flag:

  CHARISMA_CONNECTION_STRING="postgres://..." charisma migrate --connection "$CHARISMA_CONNECTION_STRING"

Notes

- By default the CLI will try to extract the datasource URL from `schema.charisma` or from `CHARISMA_CONNECTION_STRING` / `DATABASE_URL` environment variables.
- Use `--plan-only` and `--emit-sql` to review the SQL before applying it in production.
- The CLI will abort on destructive changes unless `--accept-data-loss` is supplied.
