# JSON Fields

CharismaORM has a dedicated runtime `Json` wrapper and query planner support for JSON operations.

## Runtime JSON Type

Defined at `src/Charisma.Runtime/Json/Json.cs`.

Key features:

- wraps `JsonElement`
- parse from raw string via `Json.Parse(...)`
- serialize object via `Json.FromObject(...)`
- custom converter for clean serialization behavior
- value equality based on normalized raw JSON text

## Example Usage

```csharp
var payload = Json.Parse("{""status"":""ok""}");

var created = await client.Commando.CreateAsync(new CommandoCreateArgs
{
    Data = new CommandoCreateInput
    {
        Payload = payload
    }
});
```

## Planner JSON Filters

JSON filter support includes patterns such as:

- path navigation
- string filter over extracted path text
- array predicates (`Has`, `HasEvery`, `HasSome`, length/empty checks)

These are translated to PostgreSQL JSONB expressions in planner/executor code.

## Safety Notes

- JSON filter payload values are parameterized.
- JSON path literal construction includes escaping safeguards.

## Testing References

- `tests/Charisma.QueryEngine.Tests/QueryPlannerJsonTests.cs`
- `tests/Charisma.QueryEngine.Tests/JsonFilterPlannerTests.cs`
