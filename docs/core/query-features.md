# Charisma Query Features

## Filtering

- Lambda expressions: `FindManyAsync(r => r.Name == "R-001")`
- Filter objects: `FindManyAsync(new RobotFilter { Name = "R-001" })`

## Projection

- Select: `FindManyAsync(..., select: r => new { r.Name })`
- Include: `FindManyAsync(..., include: r => r.Related)`
- Omit: `FindManyAsync(..., omit: r => r.SecretField)`

## Composite Key Support

- Use `@@id([field1, field2])` in schema for composite keys.
- Delegates generate `FindByCompositeAsync` methods.

## GroupBy & Aggregate

- `GroupByAsync(GroupByArgs)`
- `AggregateAsync(AggregateArgs)`

## JSON Filtering

- Filter on JSON fields: `FindManyAsync(r => r.Metadata["type"] == "explorer")`

## Optional Lists

- Nullable collections: `String[]?`, `Int[]?`

## Advanced Patterns

- Combine filters, projections, and includes for rich queries.
