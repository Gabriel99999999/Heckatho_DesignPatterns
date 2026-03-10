using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Domain.Entities;

public sealed class ImportSession
{
    private readonly object _sync = new();

    public ImportSession(string id, string fileName, string fileHash, string originalFilePath)
    {
        Id = id;
        FileName = fileName;
        FileHash = fileHash;
        OriginalFilePath = originalFilePath;
        CurrentFilePath = originalFilePath;
    }

    public string Id { get; }
    public string FileName { get; }
    public string FileHash { get; }
    public string OriginalFilePath { get; }
    public string CurrentFilePath { get; private set; }
    public string? LatestJobId { get; private set; }
    public ImportSnapshot? Snapshot { get; private set; }
    public SemaphoreSlim Gate { get; } = new(1, 1);

    public void SetLatestJobId(string jobId)
    {
        lock (_sync)
        {
            LatestJobId = jobId;
        }
    }

    public void SetSnapshot(ImportSnapshot snapshot, string? currentFilePath = null)
    {
        lock (_sync)
        {
            Snapshot = snapshot;
            if (!string.IsNullOrWhiteSpace(currentFilePath))
            {
                CurrentFilePath = currentFilePath;
            }
        }
    }
}
