using System.Text.Json;
using CsvCleaningService.Infrastructure.Csv;

namespace CsvCleaningService.Evaluation;

public sealed class EvalHarnessService
{
    private readonly CsvFileProcessor _csvFileProcessor;

    public EvalHarnessService(CsvFileProcessor csvFileProcessor)
    {
        _csvFileProcessor = csvFileProcessor;
    }

    public async Task<object> RunAsync(string contentRootPath, CancellationToken ct)
    {
        var fixtureDirectory = Path.Combine(contentRootPath, "Evaluation", "Fixtures");
        var specFiles = Directory.GetFiles(fixtureDirectory, "*.spec.json", SearchOption.TopDirectoryOnly);
        var results = new List<object>();
        var passed = 0;

        foreach (var specFile in specFiles)
        {
            var spec = JsonSerializer.Deserialize<EvalFixtureSpec>(
                await File.ReadAllTextAsync(specFile, ct),
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                ?? throw new InvalidOperationException($"Could not parse fixture spec {specFile}.");

            var csvPath = Path.Combine(fixtureDirectory, spec.CsvFile);
            var analysis = await _csvFileProcessor.AnalyzeFileAsync(csvPath, 50_000, ct);
            var tempOutput = Path.Combine(Path.GetTempPath(), $"eval-{Guid.NewGuid():n}.csv");

            try
            {
                await _csvFileProcessor.TransformFileAsync(csvPath, tempOutput, analysis.SuggestedRules, ct);
                var cleaned = await _csvFileProcessor.AnalyzeFileAsync(tempOutput, 50_000, ct);

                var actualRuleKeys = analysis.SuggestedRules
                    .Select(rule => $"{rule.Operation}:{rule.Column}")
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                var actualAnomalyCodes = analysis.Anomalies
                    .Select(anomaly => anomaly.Code)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);
                var cleanedAnomalyCodes = cleaned.Anomalies
                    .Select(anomaly => anomaly.Code)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var missingRules = spec.ExpectedRules.Where(rule => !actualRuleKeys.Contains(rule)).ToList();
                var missingAnomalies = spec.ExpectedAnomalies.Where(code => !actualAnomalyCodes.Contains(code)).ToList();
                var unresolvedAfterCleaning = spec.ClearedAnomalies.Where(code => cleanedAnomalyCodes.Contains(code)).ToList();
                var ok = missingRules.Count == 0 && missingAnomalies.Count == 0 && unresolvedAfterCleaning.Count == 0;

                if (ok)
                {
                    passed++;
                }

                results.Add(new
                {
                    fixture = spec.Name,
                    ok,
                    provider = analysis.SuggestionProvider,
                    benchmark = analysis.Benchmark,
                    missingRules,
                    missingAnomalies,
                    unresolvedAfterCleaning
                });
            }
            finally
            {
                if (File.Exists(tempOutput))
                {
                    File.Delete(tempOutput);
                }
            }
        }

        return new
        {
            fixtureCount = specFiles.Length,
            passed,
            failed = specFiles.Length - passed,
            results
        };
    }

    private sealed record EvalFixtureSpec(string Name, string CsvFile, List<string> ExpectedRules, List<string> ExpectedAnomalies, List<string> ClearedAnomalies);
}
