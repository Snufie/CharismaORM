# Schema DSL

The schema DSL is parsed by `RoslynSchemaParser` and converted into `CharismaSchema` IR.

## Top-Level Blocks

Supported blocks:

- `datasource`
- `generator`
- `enum`
- `model`

Example:

```charisma
datasource db {
  provider = "postgresql"
  url = env("DATABASE_URL")
}

generator client {
  provider = "charisma-generator"
  output = "./Generated"
}

enum RobotStatus {
  ACTIVE
  IDLE
}

model Robot {
  id     Id @id @default(uuid())
  name   String
  status RobotStatus
}
```

## Scalar Types

Implemented scalar set:

- `String`
- `Int`
- `Boolean`
- `Float`
- `DateTime`
- `Json`
- `Bytes`
- `Decimal`
- `BigInt`
- `UUID`
- `Id`

## Field Shapes

Supported type modifiers:

- Optional: `Type?`
- List: `Type[]`
- Optional list: `Type[]?`

## Field Attributes

Known attributes:

- `@id`
- `@unique`
- `@updatedAt`
- `@default(...)`
- `@relation(...)`

Additional attributes (such as `@db.*`) are preserved as raw attribute tokens for downstream use.

## Defaults

Recognized defaults include:

- `@default(autoincrement())`
- `@default(uuid())`
- `@default(uuidv7())`
- `@default(now())`
- static values and JSON literals (with type checks)

Validation enforces compatibility between field type and default type.

## Relations

Common accepted shapes:

```charisma
@relation("Name")
@relation(fk(Model.localField), pk(Target.id))
@relation(fk: (Model.localField), pk: (Target.id), name: "Name", onDelete: Cascade)
```

`onDelete` supports:

- `Cascade`
- `SetNull`
- `Restrict`
- `NoAction`

## Model Directives

Supported directives:

- `@@id([a, b], name: "...")`
- `@@unique([a, b], name: "...")`
- `@@index([a, b], name: "...")`

These become typed structures in model metadata and influence query uniqueness, filtering, and migration planning.

## Datasource and Generator Blocks

### `datasource` block

The `datasource` block configures how the runtime and generator connect to your database. Minimal required keys:

- `provider` — e.g. `"postgresql"`.
- `url` — a connection string or `env("NAME")` reference.

Example:

```charisma
datasource db {
  provider = "postgresql"
  url = env("DATABASE_URL")
}
```

Notes:

- `env("NAME")` is resolved by the CLI/runtime using environment variables such as `CHARISMA_CONNECTION_STRING` and `DATABASE_URL`.

### `generator` block

The `generator` block contains config consumed by the code generator. Typical keys:

- `provider` — generator identifier (e.g. `charisma-generator`).
- `output` — path where generated code is written.
- options such as `root_namespace` or format settings may be present.

Example:

```charisma
generator client {
  provider = "charisma-generator"
  output = "./Generated"
}
```

## Normalization and Hashing

`CharismaSchema` computes:

- canonical normalized text (`CanonicalText`)
- schema hash (`SchemaHash`)

This normalizer enforces deterministic ordering and formatting for reproducible generation.

## Validation Patterns

Validation includes checks for:

- duplicate models/enums/fields
- missing or conflicting primary keys
- invalid default/type combinations
- malformed relation endpoint syntax
- invalid model directives

See parser tests in `tests/Charisma.Parser.Tests` for edge and regression behavior.
