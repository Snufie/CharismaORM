using System;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Charisma.Runtime;

/// <summary>
/// Lightweight JSON value wrapper that preserves the underlying JsonElement while offering simple helpers.
/// </summary>
[JsonConverter(typeof(JsonJsonConverter))]
public readonly struct Json : IEquatable<Json>
{
    private readonly JsonElement _element;
    private readonly bool _hasValue;

    /// <summary>
    /// Creates a Json wrapper from an existing <see cref="JsonElement"/>.
    /// </summary>
    /// <param name="element">Source element to wrap.</param>
    public Json(JsonElement element)
    {
        _element = element.Clone();
        _hasValue = true;
    }

    private Json(JsonElement element, bool hasValue)
    {
        _element = element;
        _hasValue = hasValue;
    }

    /// <summary>
    /// Builds a Json wrapper from a <see cref="JsonElement"/> without exposing the original instance.
    /// </summary>
    /// <param name="element">Element to wrap.</param>
    /// <returns>Wrapped Json value.</returns>
    public static Json FromElement(JsonElement element) => new(element.Clone());

    /// <summary>
    /// Parses a JSON string into a Json wrapper; treats null/whitespace as <c>null</c> JSON.
    /// </summary>
    /// <param name="json">JSON payload to parse.</param>
    /// <returns>Wrapped Json value.</returns>
    public static Json Parse(string json)
    {
        using var doc = JsonDocument.Parse(string.IsNullOrWhiteSpace(json) ? "null" : json);
        return new Json(doc.RootElement.Clone());
    }

    /// <summary>
    /// Serializes a CLR value into a Json wrapper using optional serializer options.
    /// </summary>
    /// <typeparam name="T">Type of value to serialize.</typeparam>
    /// <param name="value">Value to serialize.</param>
    /// <param name="options">Optional serializer options.</param>
    /// <returns>Wrapped Json value.</returns>
    public static Json FromObject<T>(T value, JsonSerializerOptions? options = null)
    {
        return new Json(JsonSerializer.SerializeToElement(value, options).Clone());
    }

    /// <summary>
    /// Gets the underlying element, or default if the wrapper is empty.
    /// </summary>
    public JsonElement Element => _hasValue ? _element : default;

    /// <summary>
    /// Gets the value kind of the wrapped element.
    /// </summary>
    public JsonValueKind ValueKind => _hasValue ? _element.ValueKind : JsonValueKind.Undefined;

    /// <summary>
    /// Indicates whether the value is null or absent.
    /// </summary>
    public bool IsNull => !_hasValue || _element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined;

    /// <summary>
    /// Retrieves the raw JSON text for the value.
    /// </summary>
    public string RawText => GetRawText();

    /// <summary>
    /// Gets the raw JSON text, returning "null" when no value is present.
    /// </summary>
    /// <returns>Raw JSON representation.</returns>
    public string GetRawText() => _hasValue ? _element.GetRawText() : "null";

    public override string ToString() => GetRawText();

    /// <summary>
    /// Attempts to read a child property from an object value.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <param name="value">Outputs the wrapped child when found.</param>
    /// <returns>True when the property exists and was read.</returns>
    public bool TryGetProperty(string name, out Json value)
    {
        if (_hasValue && _element.ValueKind == JsonValueKind.Object && _element.TryGetProperty(name, out var child))
        {
            value = new Json(child.Clone());
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Produces a clone of the underlying element for external consumption.
    /// </summary>
    /// <returns>Cloned element or default.</returns>
    public JsonElement ToElement() => _hasValue ? _element.Clone() : default;

    /// <inheritdoc />
    public bool Equals(Json other)
    {
        if (_hasValue != other._hasValue)
        {
            return false;
        }

        if (!_hasValue)
        {
            return true;
        }

        return JsonElementEqualityComparer.Equals(_element, other._element);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is Json other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => _hasValue ? JsonElementEqualityComparer.GetHashCode(_element) : 0;

    public static bool operator ==(Json left, Json right) => left.Equals(right);

    public static bool operator !=(Json left, Json right) => !left.Equals(right);

    public static implicit operator Json(JsonElement element) => new(element);

    public static implicit operator JsonElement(Json json) => json.ToElement();

    public static implicit operator Json(string json) => Parse(json);

    /// <summary>
    /// Serializes the wrapped element directly so logs stay readable (no helper fields).
    /// </summary>
    public sealed class JsonJsonConverter : JsonConverter<Json>
    {
        /// <summary>
        /// Reads a Json value from the reader into a wrapper.
        /// </summary>
        /// <param name="reader">UTF-8 JSON reader.</param>
        /// <param name="typeToConvert">Target type (ignored).</param>
        /// <param name="options">Serializer options.</param>
        /// <returns>Wrapped Json value.</returns>
        public override Json Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            using var doc = JsonDocument.ParseValue(ref reader);
            return new Json(doc.RootElement.Clone());
        }

        /// <summary>
        /// Writes the wrapped Json element as-is or null when empty.
        /// </summary>
        /// <param name="writer">Destination writer.</param>
        /// <param name="value">Value to write.</param>
        /// <param name="options">Serializer options.</param>
        public override void Write(Utf8JsonWriter writer, Json value, JsonSerializerOptions options)
        {
            // If no value, write null; otherwise write the wrapped element as-is.
            if (value.IsNull)
            {
                writer.WriteNullValue();
                return;
            }

            value._element.WriteTo(writer);
        }
    }

    private static class JsonElementEqualityComparer
    {
        /// <summary>
        /// Compares two <see cref="JsonElement"/> instances by raw JSON text.
        /// </summary>
        /// <param name="left">First element.</param>
        /// <param name="right">Second element.</param>
        /// <returns>True when value kinds and raw text match.</returns>
        public static bool Equals(JsonElement left, JsonElement right)
        {
            return left.ValueKind == right.ValueKind && left.GetRawText().Equals(right.GetRawText(), StringComparison.Ordinal);
        }

        /// <summary>
        /// Computes a hash code for a <see cref="JsonElement"/> based on its raw JSON.
        /// </summary>
        /// <param name="element">Element to hash.</param>
        /// <returns>Hash code.</returns>
        public static int GetHashCode(JsonElement element)
        {
            return element.GetRawText().GetHashCode(StringComparison.Ordinal);
        }
    }
}
