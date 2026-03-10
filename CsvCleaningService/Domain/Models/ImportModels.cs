namespace CsvCleaningService.Domain.Models;

public record ProcessingOptions(int WorkerCount, int QueueCapacity, int MaxUniqueTracking);
public record SavedUpload(string OriginalFileName, string FileHash, string StoredPath);
public record ImportSnapshot(int RowCount, List<string> Headers, List<Dictionary<string, string>> PreviewRows, List<ColumnProfile> Profile, List<Anomaly> Anomalies, List<TransformRule> SuggestedRules, long ExecutionMs, double RowsPerSecond);
public record CsvAnalysisResult(List<string> Headers, int RowCount, List<Dictionary<string, string>> PreviewRows, List<ColumnProfile> Profile, List<Anomaly> Anomalies, List<TransformRule> SuggestedRules, long ExecutionMs);
public record TransformRequest(List<TransformRule>? Rules);
public record TransformRule(string Operation, string Column, string? Parameter, bool Enabled = true);
public record ColumnProfile(string Column, string InferredType, int TotalRows, int NonEmptyRows, double NullRate, int UniqueCount, double? Min, double? Max, int MaxLength, List<string> Samples);
public record Anomaly(string Column, string Code, string Message, int Count);
public record OutboxMessage(string Id, string ImportId, JobType Type, List<TransformRule> Rules, JobStatus Status, DateTimeOffset CreatedAtUtc, DateTimeOffset? StartedAtUtc, DateTimeOffset? CompletedAtUtc, long? ExecutionMs, double? RowsPerSecond, string? Error);

public enum JobType
{
    Profile,
    Transform
}

public enum JobStatus
{
    Pending,
    Processing,
    Completed,
    Failed
}
