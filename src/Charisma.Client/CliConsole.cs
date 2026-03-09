using System;

namespace Charisma.Client;

/// <summary>
/// Minimal colored console helpers for a friendlier CLI.
/// </summary>
internal static class CliConsole
{
    public static void Info(string message) => WriteWithColor(message, ConsoleColor.Gray);
    public static void Success(string message) => WriteWithColor(message, ConsoleColor.Green);
    public static void Warn(string message) => WriteWithColor(message, ConsoleColor.Yellow);
    public static void Error(string message) => WriteWithColor(message, ConsoleColor.Red, isError: true);
    public static void Step(int index, bool destructive, string message)
    {
        var prefix = destructive ? "!" : "-";
        var color = destructive ? ConsoleColor.Yellow : ConsoleColor.Cyan;
        WriteWithColor($"  [{index}] {prefix} {message}", color);
    }

    public static void Bullet(string message) => WriteWithColor($"  • {message}", ConsoleColor.DarkGray);

    private static void WriteWithColor(string message, ConsoleColor color, bool isError = false)
    {
        var original = Console.ForegroundColor;
        Console.ForegroundColor = color;
        if (isError)
        {
            Console.Error.WriteLine(message);
        }
        else
        {
            Console.WriteLine(message);
        }
        Console.ForegroundColor = original;
    }
}
