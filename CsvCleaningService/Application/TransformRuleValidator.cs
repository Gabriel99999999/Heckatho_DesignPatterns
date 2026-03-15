using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Application;

public sealed class TransformRuleValidator
{
    private static readonly HashSet<string> AllowedOperations =
    [
        "trim",
        "tolower",
        "toupper",
        "nullifempty",
        "normalizestatus",
        "normalizecity",
        "normalizephone",
        "removeinvalidphone",
        "removeinvalidemail",
        "parsenumber",
        "normalizedate",
        "rename"
    ];

    public List<TransformRule> Validate(IEnumerable<TransformRule> rules, IReadOnlyCollection<string> headers)
    {
        var validHeaders = new HashSet<string>(headers, StringComparer.OrdinalIgnoreCase);
        var validated = new List<TransformRule>();

        foreach (var rule in rules)
        {
            if (string.IsNullOrWhiteSpace(rule.Operation) || string.IsNullOrWhiteSpace(rule.Column))
            {
                continue;
            }

            if (!AllowedOperations.Contains(rule.Operation.ToLowerInvariant()))
            {
                continue;
            }

            if (!validHeaders.Contains(rule.Column))
            {
                continue;
            }

            if (rule.Operation.Equals("rename", StringComparison.OrdinalIgnoreCase)
                && string.IsNullOrWhiteSpace(rule.Parameter))
            {
                continue;
            }

            validated.Add(rule);
        }

        return validated;
    }
}
