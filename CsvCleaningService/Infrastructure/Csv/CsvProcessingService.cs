using CsvCleaningService.Domain.Entities;
using CsvCleaningService.Domain.Models;
using CsvCleaningService.Infrastructure.Storage;
using CsvCleaningService.Infrastructure.Stores;

namespace CsvCleaningService.Infrastructure.Csv;

public sealed class CsvProcessingService
{
    private readonly ImportStore _store;
    private readonly FileStorage _storage;
    private readonly ProcessingOptions _options;
    private readonly CsvFileProcessor _csvFileProcessor;

    public CsvProcessingService(ImportStore store, FileStorage storage, ProcessingOptions options, CsvFileProcessor csvFileProcessor)
    {
        _store = store;
        _storage = storage;
        _options = options;
        _csvFileProcessor = csvFileProcessor;
    }

    public async Task<int> ProcessAsync(OutboxMessage message, CancellationToken ct)
    {
        if (!_store.TryGet(message.ImportId, out var session))
        {
            return 0;
        }

        await session.Gate.WaitAsync(ct);
        try
        {
            return message.Type switch
            {
                JobType.Profile => await ProcessProfileAsync(session, ct),
                JobType.Transform => await ProcessTransformAsync(session, message.Rules, ct),
                _ => 0
            };
        }
        finally
        {
            session.Gate.Release();
        }
    }

    private async Task<int> ProcessProfileAsync(ImportSession session, CancellationToken ct)
    {
        var analysis = await _csvFileProcessor.AnalyzeFileAsync(session.CurrentFilePath, _options.MaxUniqueTracking, ct);
        session.SetSnapshot(ToSnapshot(analysis));
        return analysis.RowCount;
    }

    private async Task<int> ProcessTransformAsync(ImportSession session, List<TransformRule> rules, CancellationToken ct)
    {
        var outputPath = _storage.CreateDerivedCsvPath(session.Id);
        var transformBenchmark = await _csvFileProcessor.TransformFileAsync(session.OriginalFilePath, outputPath, rules, ct);

        var analysis = await _csvFileProcessor.AnalyzeFileAsync(outputPath, _options.MaxUniqueTracking, ct);
        var oldPath = session.CurrentFilePath;
        session.SetSnapshot(ToSnapshot(analysis, transformBenchmark), outputPath);

        if (!string.Equals(oldPath, session.OriginalFilePath, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(oldPath, outputPath, StringComparison.OrdinalIgnoreCase))
        {
            _storage.TryDelete(oldPath);
        }

        return analysis.RowCount;
    }

    private static ImportSnapshot ToSnapshot(CsvAnalysisResult analysis, BenchmarkMetrics? overrideBenchmark = null)
    {
        return new ImportSnapshot(
            analysis.RowCount,
            analysis.Headers,
            analysis.PreviewRows,
            analysis.Profile,
            analysis.Anomalies,
            analysis.SuggestedRules,
            overrideBenchmark ?? analysis.Benchmark,
            analysis.SuggestionProvider);
    }
}
