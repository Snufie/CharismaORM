# Charisma Optional Lists

Charisma models can have optional lists, which are nullable collections.

## Why Optional Lists?

- Some databases allow null for list fields (e.g., array columns).
- Useful for representing missing or undefined relationships.
- Enables flexible modeling of real-world data.

## Example

```charisma
model Robot {
  tags String[]? // Optional list of tags
}
```

## Usage in C\#

```csharp
var robot = await client.Robot.FindByIdAsync(...);
if (robot.Tags != null)
{
    foreach (var tag in robot.Tags)
    {
        // ...
    }
}
```

## Tips

- Use optional lists for fields that may be missing or undefined.
- Required lists use `Type[]` (non-nullable).
- Required lists can still default to `[]` (empty list).
