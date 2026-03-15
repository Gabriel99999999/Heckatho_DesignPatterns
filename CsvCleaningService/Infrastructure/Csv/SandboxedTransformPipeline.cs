using System.Globalization;
using CsvCleaningService.Application;
using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Infrastructure.Csv;

public sealed class SandboxedTransformPipeline
{
    private readonly ContactValueValidator _validator;
    private readonly TransformRuleValidator _ruleValidator;
    private readonly Dictionary<string, Func<string, string?, string>> _tools;

    public SandboxedTransformPipeline(ContactValueValidator validator, TransformRuleValidator ruleValidator)
    {
        _validator = validator;
        _ruleValidator = ruleValidator;
        _tools = new Dictionary<string, Func<string, string?, string>>(StringComparer.OrdinalIgnoreCase)
        {
            ["trim"] = static (value, _) => value.Trim(),
            ["tolower"] = static (value, _) => value.ToLowerInvariant(),
            ["toupper"] = static (value, _) => value.ToUpperInvariant(),
            ["nullifempty"] = static (value, _) => string.IsNullOrWhiteSpace(value) ? string.Empty : value,
            ["normalizestatus"] = static (value, _) => NormalizeStatus(value),
            ["normalizecity"] = static (value, _) => NormalizeCity(value),
            ["normalizephone"] = static (value, _) => NormalizePhone(value),
            ["removeinvalidphone"] = (value, _) => _validator.IsValidPhone(value) ? value : string.Empty,
            ["removeinvalidemail"] = (value, _) => _validator.IsValidEmail(value) ? value : string.Empty,
            ["parsenumber"] = static (value, _) => ParseNumber(value),
            ["normalizedate"] = static (value, _) => NormalizeDate(value)
        };
    }

    public List<TransformRule> ValidateRules(IEnumerable<TransformRule> rules, IReadOnlyCollection<string> headers)
        => _ruleValidator.Validate(rules, headers);

    public string Apply(TransformRule rule, string value)
    {
        if (_tools.TryGetValue(rule.Operation, out var tool))
        {
            return tool(value, rule.Parameter);
        }

        return value;
    }

    private static string NormalizeStatus(string value)
    {
        return value.Trim().ToLowerInvariant() switch
        {
            "lost" => "Lost",
            "found" => "Found",
            _ => value.Trim()
        };
    }

    private static string NormalizeCity(string value)
    {
        var trimmed = value.Trim();
        return string.IsNullOrWhiteSpace(trimmed)
            ? string.Empty
            : CultureInfo.InvariantCulture.TextInfo.ToTitleCase(trimmed.ToLowerInvariant());
    }

    private static string NormalizePhone(string value)
    {
        var trimmed = value.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
        {
            return string.Empty;
        }

        if (trimmed.StartsWith("0049", StringComparison.Ordinal))
        {
            trimmed = $"+49{trimmed[4..]}";
        }

        return trimmed.Replace(" ", string.Empty);
    }

    private static string ParseNumber(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Equals("unknown", StringComparison.OrdinalIgnoreCase))
        {
            return string.Empty;
        }

        return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed.ToString("0.###", CultureInfo.InvariantCulture)
            : string.Empty;
    }

    private static string NormalizeDate(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var unix) && unix > 1000000000)
        {
            try
            {
                return DateTimeOffset.FromUnixTimeSeconds((long)unix).UtcDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }
            catch
            {
                return string.Empty;
            }
        }

        var formats = new[] { "yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "MM/dd/yyyy" };
        if (DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var exactDate))
        {
            return exactDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        return DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsedDate)
            ? parsedDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : string.Empty;
    }
}
