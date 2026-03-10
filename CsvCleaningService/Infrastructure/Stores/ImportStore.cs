using System.Collections.Concurrent;
using CsvCleaningService.Domain.Entities;

namespace CsvCleaningService.Infrastructure.Stores;

public sealed class ImportStore
{
    private readonly ConcurrentDictionary<string, ImportSession> _sessions = new();
    private readonly ConcurrentDictionary<string, string> _importIdByHash = new(StringComparer.OrdinalIgnoreCase);

    public void Save(ImportSession session)
    {
        _sessions[session.Id] = session;
        _importIdByHash[session.FileHash] = session.Id;
    }

    public bool TryGet(string id, out ImportSession session) => _sessions.TryGetValue(id, out session!);

    public bool TryGetByHash(string hash, out ImportSession session)
    {
        session = null!;
        if (!_importIdByHash.TryGetValue(hash, out var importId))
        {
            return false;
        }

        return _sessions.TryGetValue(importId, out session!);
    }
}
