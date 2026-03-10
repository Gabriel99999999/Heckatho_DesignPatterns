using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Application;

public sealed class TransformSuggestionService
{
    public List<TransformRule> Suggest(List<string> headers, List<Anomaly> anomalies)
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

            if (header.Contains("status", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizeStatus", header, null, true));
            }

            if (header.Contains("city", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizeCity", header, null, true));
            }

            if (header.Contains("phone", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizePhone", header, null, true));
                result.Add(new TransformRule("removeInvalidPhone", header, null, true));
            }

            if (header.Contains("date", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("normalizeDate", header, null, true));
            }

            if (header.Contains("value", StringComparison.OrdinalIgnoreCase)
                || anomalies.Any(a => a.Column.Equals(header, StringComparison.OrdinalIgnoreCase) && a.Code == "mixed_types"))
            {
                result.Add(new TransformRule("parseNumber", header, null, true));
            }

            result.Add(new TransformRule("nullIfEmpty", header, null, true));
        }

        return result
            .GroupBy(rule => $"{rule.Operation}|{rule.Column}|{rule.Parameter}", StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();
    }
}
