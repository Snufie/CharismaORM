using System;
using System.Globalization;

namespace Charisma.Runtime;

/// <summary>
/// Utilities for working with UUIDv7 values.
/// </summary>
public static class UuidV7Timestamp
{
    /// <summary>
    /// Extracts the embedded creation timestamp from a UUIDv7 value in UTC.
    /// </summary>
    /// <param name="value">The UUIDv7 value.</param>
    /// <returns>The UTC timestamp encoded in the UUID.</returns>
    /// <exception cref="ArgumentException">Thrown when the supplied Guid is not version 7.</exception>
    public static DateTimeOffset ExtractCreatedAtUtc(Guid value)
    {
        var hex = value.ToString("N", CultureInfo.InvariantCulture);
        // UUIDv7 uses the first 48 bits (12 hex chars) as Unix epoch milliseconds.
        var millisHex = hex[..12];

        // Version nibble is the first char of the 3rd group in canonical text form.
        var versionNibble = hex[12];
        if (versionNibble != '7')
        {
            throw new ArgumentException("The provided Guid is not a UUIDv7 value.", nameof(value));
        }

        var unixMillis = Convert.ToInt64(millisHex, 16);
        return DateTimeOffset.FromUnixTimeMilliseconds(unixMillis);
    }
}
