using System.Text.RegularExpressions;

namespace CsvCleaningService.Infrastructure.Csv;

public sealed partial class ContactValueValidator
{
    [GeneratedRegex(@"^[^\s@]+@[^\s@]+\.[^\s@]+$", RegexOptions.IgnoreCase)]
    private static partial Regex EmailPattern();

    [GeneratedRegex(@"^[+\d][\d\s()-]{6,}$", RegexOptions.IgnoreCase)]
    private static partial Regex PhonePattern();

    public bool IsValidEmail(string value)
        => !string.IsNullOrWhiteSpace(value) && EmailPattern().IsMatch(value.Trim());

    public bool IsValidPhone(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        var trimmed = value.Trim();
        if (!PhonePattern().IsMatch(trimmed))
        {
            return false;
        }

        return trimmed.Count(char.IsDigit) >= 7;
    }
}
