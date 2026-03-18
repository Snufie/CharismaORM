# JSON Fields

Charisma provides a runtime `Json` wrapper type and planner/executor support for JSON operations. This page documents the runtime type, common usage, and safety guidance.

## Runtime `Json` type

Location: `src/Charisma.Runtime/Json/Json.cs`.

Key behaviors:

- Wraps `JsonElement` and provides `Parse`/`FromObject` helpers.
- Normalizes JSON text for deterministic equality and hashing.
- Includes a custom converter to integrate with the generated models and the serializer used by the runtime.

Example usage

```csharp
var payload = Json.Parse("{\"status\":\"ok\"}");

var created = await client.Commando.CreateAsync(new CommandoCreateArgs
{
    Data = new CommandoCreateInput { Payload = payload }
});
```

## Planner interaction

The query planner understands JSON filters (see `json-filtering.md`) and translates high-level predicates to provider JSONB expressions for Postgres.

## Safety and performance

- JSON values used in filters are always parameterized.
- For frequent JSON queries, create appropriate indexes (GIN on `jsonb`) and review explain plans.

## Tests

- `tests/Charisma.QueryEngine.Tests/QueryPlannerJsonTests.cs`
- `tests/Charisma.QueryEngine.Tests/JsonFilterPlannerTests.cs`
