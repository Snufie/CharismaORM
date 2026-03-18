# Code Generation

Code generation is orchestrated by `CharismaGenerator` and writer components under `src/Charisma.Generator/Writers`.

## Generator Inputs

- validated `CharismaSchema`
- `GeneratorOptions`:
  - `RootNamespace`
  - `GeneratorVersion`
  - `OutputDirectory`

## Output Structure

Generated artifacts include:

- `Enums/*.g.cs`
- `Models/*.g.cs`
- `Delegates/*Delegate.g.cs`
- `Metadata/ModelMetadataRegistry.g.cs`
- `Args/*Args.g.cs`
- `Filters/*Filter.g.cs`
- `Select/*Select.g.cs`
- `Omit/*Omit.g.cs`
- `Include/*Include.g.cs`
- `CharismaClient.g.cs`

## Writer Responsibilities

- `EnumWriter`: enum type emission
- `ModelWriter`: model POCOs and scalar mapping
- `DelegateWriter`: delegate methods and convenience APIs
- `MetadataWriter`: runtime metadata registry
- `ArgsWriter`: operation args and payload classes
- `FilterWriter`: where and scalar filter types
- `SelectWriter`: projection masks
- `OmitWriter`: omission masks and global omit options
- `IncludeWriter`: include graph types
- `ClientWriter`: root `CharismaClient`

## Generated Headers and Determinism

Every generated file contains:

- auto-generated warning header
- generator version
- schema hash
- `#nullable enable`

This supports:

- reproducibility
- regeneration consistency
- debug traceability

## Generated API Surface (Per Model)

Typical delegate methods:

- `FindUniqueAsync`
- `FindFirstAsync`
- `FindFirstOrThrowAsync`
- `FindManyAsync`
- `CreateAsync`
- `CreateManyAsync`
- `CreateManyAndReturnAsync`
- `UpdateAsync`
- `UpdateManyAsync`
- `UpdateManyAndReturnAsync`
- `DeleteAsync`
- `DeleteManyAsync`
- `UpsertAsync`
- `CountAsync`
- `AggregateAsync`
- `GroupByAsync`

Convenience methods are generated based on key shape (single/composite primary key and selector support).

## Type Mapping Notes

Core scalar-to-C# mapping includes:

- `Id`/`UUID` -> `Guid`
- `String` -> `string`
- `Int` -> `int`
- `Float` -> `double`
- `Decimal` -> `decimal`
- `Boolean` -> `bool`
- `DateTime` -> `DateTime`
- `Json` -> `Json`
- `Bytes` -> `byte[]`

Optional/list flags map to nullable/list forms.

## Regeneration Workflow

Recommended loop:

1. edit schema
2. run `charisma generate`
3. build
4. resolve type/usage changes in app code

## Test Coverage

Generator behavior is exercised in:

- `tests/Charisma.Generator.Tests/GeneratorIntegrationTests.cs`
- `tests/Charisma.Generator.Tests/GeneratedClientIntegrationTests.cs`
- `tests/Charisma.Generator.Tests/GeneratorRegressionTests.cs`
- `tests/Charisma.Generator.Tests/FileNameDerivationTests.cs`
