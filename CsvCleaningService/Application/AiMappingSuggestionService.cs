using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Application;

public sealed class AiMappingSuggestionService
{
    private const string ProviderName = "local-heuristic-llm";

    private readonly TransformRuleValidator _validator;

    public AiMappingSuggestionService(TransformRuleValidator validator)
    {
        _validator = validator;
    }

    public (List<TransformRule> Rules, string Provider) Suggest(List<string> headers, List<ColumnProfile> profile, List<Anomaly> anomalies)
    {
        var result = new List<TransformRule>();

        foreach (var header in headers)
        {
            result.Add(new TransformRule("trim", header, null, true));

            if (header.Contains("email", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("toLower", header, null, true));
                result.Add(new TransformRule("removeInvalidEmail", header, null, true));
            }

            if (header.Contains("phone", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizePhone", header, null, true));
                result.Add(new TransformRule("removeInvalidPhone", header, null, true));
            }

            if (header.Contains("status", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizeStatus", header, null, true));
            }

            if (header.Contains("city", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizeCity", header, null, true));
            }

            if (header.Contains("date", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizeDate", header, null, true));
            }

            if (ShouldParseNumber(header, anomalies))
            {
                result.Add(new TransformRule("parseNumber", header, null, true));
            }

            if (profile.Any(item => item.Column.Equals(header, StringComparison.OrdinalIgnoreCase) && item.NullRate > 0))
            {
                result.Add(new TransformRule("nullIfEmpty", header, null, true));
            }
        }

        var uniqueRules = result
            .GroupBy(rule => $"{rule.Operation}|{rule.Column}|{rule.Parameter}", StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();

        return (_validator.Validate(uniqueRules, headers), ProviderName);
    }

    private static bool ShouldParseNumber(string header, List<Anomaly> anomalies)
    {
        return header.Contains("value", StringComparison.OrdinalIgnoreCase)
            || anomalies.Any(anomaly => anomaly.Column.Equals(header, StringComparison.OrdinalIgnoreCase) && anomaly.Code == "mixed_types");
    }
}
