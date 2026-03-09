using System;
using System.Text;
using System.Collections.Generic;

namespace Charisma.Migration.Postgres;

internal static class IdentifierCasing
{
    public static string ToModelName(string input) => Pascalize(input);

    public static string ToEnumName(string input) => Pascalize(input);

    public static string ToFieldName(string input)
    {
        if (string.IsNullOrEmpty(input)) return input;

        var lower = input.ToLowerInvariant();
        if (lower == "created_at") return "createdAt";
        if (lower == "updated_at") return "updatedAt";

        var pascal = PascalizeSmart(input);
        var camel = char.ToLowerInvariant(pascal[0]) + pascal.Substring(1);
        camel = FilterToIdentifier(camel);

        if (string.IsNullOrEmpty(camel))
        {
            return "field";
        }

        if (!char.IsLetter(camel[0]))
        {
            camel = "f" + camel;
        }

        return camel;
    }

    public static string Pascalize(string input) => PascalizeSmart(input);

    public static string Pluralize(string input)
    {
        if (string.IsNullOrEmpty(input)) return input;
        if (input.EndsWith("s", StringComparison.OrdinalIgnoreCase)) return input;
        return input + "s";
    }

    private static string PascalizeSmart(string input)
    {
        if (string.IsNullOrEmpty(input)) return input;

        var tokens = SplitTokensSmart(input);
        var sb = new StringBuilder();
        foreach (var token in tokens)
        {
            if (token.Length == 0) continue;
            sb.Append(char.ToUpperInvariant(token[0]));
            if (token.Length > 1)
            {
                sb.Append(token.Substring(1));
            }
        }

        return sb.Length == 0 ? input : sb.ToString();
    }

    private static IReadOnlyList<string> SplitTokensSmart(string input)
    {
        var result = new List<string>();
        var parts = input.Split(new[] { '_', ' ', '-' }, StringSplitOptions.RemoveEmptyEntries);

        foreach (var part in parts)
        {
            foreach (var token in SplitCaseAndLower(part))
            {
                if (!string.IsNullOrEmpty(token))
                {
                    result.Add(token.ToLowerInvariant());
                }
            }
        }

        return result;
    }

    private static IEnumerable<string> SplitCaseAndLower(string part)
    {
        if (string.IsNullOrEmpty(part)) yield break;

        var current = new StringBuilder();
        for (int i = 0; i < part.Length; i++)
        {
            var ch = part[i];
            var prev = i > 0 ? part[i - 1] : '\0';
            var next = i < part.Length - 1 ? part[i + 1] : '\0';

            var boundary = false;
            if (i > 0)
            {
                if (char.IsLower(prev) && char.IsUpper(ch)) boundary = true;
                else if (char.IsUpper(prev) && char.IsUpper(ch) && char.IsLower(next)) boundary = true;
                else if (char.IsDigit(prev) && char.IsLetter(ch)) boundary = true;
                else if (char.IsLetter(prev) && char.IsDigit(ch)) boundary = true;
            }

            if (boundary)
            {
                yield return current.ToString();
                current.Clear();
            }

            current.Append(ch);
        }

        if (current.Length > 0)
        {
            var token = current.ToString();
            foreach (var split in SplitLowerCompound(token))
            {
                yield return split;
            }
        }
    }

    private static IEnumerable<string> SplitLowerCompound(string token)
    {
        const StringComparison cmp = StringComparison.OrdinalIgnoreCase;
        var suffixes = new[]
        {
            "percentage",
            "status",
            "count",
            "number",
            "total",
            "code",
            "type",
            "name",
            "email",
            "date",
            "time",
            "amount",
            "value",
            "level",
            "score",
            "rate",
            "index",
            "flag"
        };

        foreach (var suffix in suffixes)
        {
            if (token.Length > suffix.Length && token.EndsWith(suffix, cmp))
            {
                var head = token.Substring(0, token.Length - suffix.Length);
                foreach (var headPart in SplitLowerCompound(head))
                {
                    yield return headPart;
                }
                yield return suffix;
                yield break;
            }
        }

        yield return token;
    }

    private static string FilterToIdentifier(string input)
    {
        if (string.IsNullOrEmpty(input)) return input;

        var sb = new StringBuilder(input.Length);
        foreach (var ch in input)
        {
            if (char.IsLetterOrDigit(ch) || ch == '_')
            {
                sb.Append(ch);
            }
        }

        return sb.ToString();
    }
}
