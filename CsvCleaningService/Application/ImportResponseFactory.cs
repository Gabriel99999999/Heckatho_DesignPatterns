using CsvCleaningService.Domain.Entities;

namespace CsvCleaningService.Application;

public sealed class ImportResponseFactory
{
    public static object BuildImportResponse(ImportSession session, bool deduplicated)
    {
        var snapshot = session.Snapshot!;

        return new
        {
            importId = session.Id,
            fileName = session.FileName,
            fileHash = session.FileHash,
            deduplicated,
            latestJobId = session.LatestJobId,
            rowCount = snapshot.RowCount,
            headers = snapshot.Headers,
            previewRows = snapshot.PreviewRows,
            profile = snapshot.Profile,
            anomalies = snapshot.Anomalies,
            suggestedRules = snapshot.SuggestedRules,
            suggestionProvider = snapshot.SuggestionProvider,
            benchmark = snapshot.Benchmark,
            executionMs = snapshot.Benchmark.ExecutionMs,
            rowsPerSecond = snapshot.Benchmark.RowsPerSecond
        };
    }
}
