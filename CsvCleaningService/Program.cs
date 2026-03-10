using System.Buffers;
using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);

var storageRoot = Path.Combine(builder.Environment.ContentRootPath, "App_Data");
Directory.CreateDirectory(storageRoot);

builder.Services.AddSingleton(new ProcessingOptions(Math.Max(2, Environment.ProcessorCount / 2), 64, 50_000));
builder.Services.AddSingleton(new FileStorage(storageRoot));
builder.Services.AddSingleton<ImportStore>();
builder.Services.AddSingleton<OutboxStore>();
builder.Services.AddSingleton<CsvProcessingService>();
builder.Services.AddHostedService<OutboxWorkerService>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapPost("/api/import/upload", async (HttpRequest request, ImportStore store, OutboxStore outbox, FileStorage storage, CancellationToken ct) =>
{
    if (!request.HasFormContentType)
    {
        return Results.BadRequest(new { error = "Expected multipart/form-data." });
    }

    var form = await request.ReadFormAsync(ct);
    var file = form.Files["file"] ?? form.Files.FirstOrDefault();
    if (file is null || file.Length == 0)
    {
        return Results.BadRequest(new { error = "CSV file is required." });
    }

    var upload = await storage.SaveUploadAsync(file, ct);

    if (store.TryGetByHash(upload.FileHash, out var existing))
    {
        storage.TryDelete(upload.StoredPath);

        if (existing.Snapshot is not null)
        {
            return Results.Ok(ResponseFactory.BuildImportResponse(existing, true));
        }

        return Results.Accepted($"/api/import/{existing.Id}", new
        {
            importId = existing.Id,
            fileName = existing.FileName,
            deduplicated = true,
            status = "processing",
            latestJobId = existing.LatestJobId
        });
    }

    var importId = Guid.NewGuid().ToString("n");
    var session = new ImportSession(importId, upload.OriginalFileName, upload.FileHash, upload.StoredPath);
    store.Save(session);

    var message = await outbox.EnqueueAsync(importId, JobType.Profile, new List<TransformRule>(), ct);
    session.SetLatestJobId(message.Id);

    return Results.Accepted($"/api/import/{importId}", new
    {
        importId,
        fileName = upload.OriginalFileName,
        deduplicated = false,
        status = "queued",
        jobId = message.Id
    });
});

app.MapPost("/api/import/{importId}/transform", async (string importId, TransformRequest request, ImportStore store, OutboxStore outbox, CancellationToken ct) =>
{
    if (!store.TryGet(importId, out var session))
    {
        return Results.NotFound(new { error = "Import session not found." });
    }

    var rules = (request.Rules ?? new List<TransformRule>()).Where(r => r.Enabled).ToList();
    var message = await outbox.EnqueueAsync(importId, JobType.Transform, rules, ct);
    session.SetLatestJobId(message.Id);

    return Results.Accepted($"/api/import/{importId}", new
    {
        importId,
        status = "queued",
        jobId = message.Id
    });
});

app.MapGet("/api/import/{importId}", (string importId, ImportStore store) =>
{
    if (!store.TryGet(importId, out var session))
    {
        return Results.NotFound(new { error = "Import session not found." });
    }

    if (session.Snapshot is null)
    {
        return Results.Ok(new
        {
            importId = session.Id,
            fileName = session.FileName,
            status = "processing",
            latestJobId = session.LatestJobId
        });
    }

    return Results.Ok(ResponseFactory.BuildImportResponse(session, false));
});

app.MapGet("/api/job/{jobId}", (string jobId, OutboxStore outbox) =>
{
    if (!outbox.TryGet(jobId, out var message))
    {
        return Results.NotFound(new { error = "Job not found." });
    }

    return Results.Ok(new
    {
        jobId = message.Id,
        importId = message.ImportId,
        type = message.Type.ToString().ToLowerInvariant(),
        status = message.Status.ToString().ToLowerInvariant(),
        createdAtUtc = message.CreatedAtUtc,
        startedAtUtc = message.StartedAtUtc,
        completedAtUtc = message.CompletedAtUtc,
        executionMs = message.ExecutionMs,
        rowsPerSecond = message.RowsPerSecond,
        error = message.Error
    });
});

app.MapGet("/api/import/{importId}/download", (string importId, string? format, ImportStore store, CancellationToken ct) =>
{
    if (!store.TryGet(importId, out var session) || session.Snapshot is null)
    {
        return Results.NotFound(new { error = "Import session not found or not ready." });
    }

    var selectedFormat = (format ?? "csv").ToLowerInvariant();
    var cleanName = BuildCleanName(session.FileName);

    if (selectedFormat == "json")
    {
        return Results.Stream(async output => await CsvUtilities.WriteJsonAsync(session.CurrentFilePath, output, ct), "application/json", $"{cleanName}.json");
    }

    return Results.File(session.CurrentFilePath, "text/csv", $"{cleanName}.csv");
});

app.Run();

static string BuildCleanName(string original)
{
    var name = Path.GetFileNameWithoutExtension(original);
    return $"clean-{(string.IsNullOrWhiteSpace(name) ? "cleaned" : name)}";
}

static class ResponseFactory
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
            executionMs = snapshot.ExecutionMs,
            rowsPerSecond = snapshot.RowsPerSecond
        };
    }
}

sealed class OutboxWorkerService : BackgroundService
{
    private readonly OutboxStore _outbox;
    private readonly CsvProcessingService _processor;
    private readonly ProcessingOptions _options;

    public OutboxWorkerService(OutboxStore outbox, CsvProcessingService processor, ProcessingOptions options)
    {
        _outbox = outbox;
        _processor = processor;
        _options = options;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var workers = Enumerable.Range(0, _options.WorkerCount)
            .Select(_ => RunWorkerAsync(stoppingToken));

        return Task.WhenAll(workers);
    }

    private async Task RunWorkerAsync(CancellationToken ct)
    {
        await foreach (var message in _outbox.ReadAllAsync(ct))
        {
            _outbox.MarkProcessing(message.Id);
            var started = DateTimeOffset.UtcNow;

            try
            {
                var rowsProcessed = await _processor.ProcessAsync(message, ct);
                var elapsedMs = Math.Max(1, (long)(DateTimeOffset.UtcNow - started).TotalMilliseconds);
                var rowsPerSecond = rowsProcessed <= 0 ? 0d : Math.Round(rowsProcessed / (elapsedMs / 1000d), 2);
                _outbox.MarkCompleted(message.Id, elapsedMs, rowsPerSecond);
            }
            catch (Exception ex)
            {
                _outbox.MarkFailed(message.Id, ex.Message);
            }
        }
    }
}

sealed class CsvProcessingService
{
    private readonly ImportStore _store;
    private readonly FileStorage _storage;
    private readonly ProcessingOptions _options;

    public CsvProcessingService(ImportStore store, FileStorage storage, ProcessingOptions options)
    {
        _store = store;
        _storage = storage;
        _options = options;
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
        var analysis = await CsvUtilities.AnalyzeFileAsync(session.CurrentFilePath, _options.MaxUniqueTracking, ct);
        session.SetSnapshot(ToSnapshot(analysis));
        return analysis.RowCount;
    }

    private async Task<int> ProcessTransformAsync(ImportSession session, List<TransformRule> rules, CancellationToken ct)
    {
        var outputPath = _storage.CreateDerivedCsvPath(session.Id);
        await CsvUtilities.TransformFileAsync(session.OriginalFilePath, outputPath, rules, ct);

        var analysis = await CsvUtilities.AnalyzeFileAsync(outputPath, _options.MaxUniqueTracking, ct);
        var oldPath = session.CurrentFilePath;
        session.SetSnapshot(ToSnapshot(analysis), outputPath);

        if (!string.Equals(oldPath, session.OriginalFilePath, StringComparison.OrdinalIgnoreCase)
            && !string.Equals(oldPath, outputPath, StringComparison.OrdinalIgnoreCase))
        {
            _storage.TryDelete(oldPath);
        }

        return analysis.RowCount;
    }

    private static ImportSnapshot ToSnapshot(CsvAnalysisResult analysis)
    {
        var elapsed = Math.Max(1, analysis.ExecutionMs);
        var rowsPerSecond = analysis.RowCount <= 0 ? 0d : Math.Round(analysis.RowCount / (elapsed / 1000d), 2);

        return new ImportSnapshot(
            analysis.RowCount,
            analysis.Headers,
            analysis.PreviewRows,
            analysis.Profile,
            analysis.Anomalies,
            analysis.SuggestedRules,
            elapsed,
            rowsPerSecond);
    }
}

sealed class ImportStore
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

sealed class OutboxStore
{
    private readonly ConcurrentDictionary<string, OutboxMessage> _messages = new();
    private readonly Channel<OutboxMessage> _queue;

    public OutboxStore(ProcessingOptions options)
    {
        _queue = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(options.QueueCapacity)
        {
            SingleReader = false,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public async Task<OutboxMessage> EnqueueAsync(string importId, JobType type, List<TransformRule> rules, CancellationToken ct)
    {
        var message = new OutboxMessage(
            Guid.NewGuid().ToString("n"),
            importId,
            type,
            rules,
            JobStatus.Pending,
            DateTimeOffset.UtcNow,
            null,
            null,
            null,
            null,
            null);

        _messages[message.Id] = message;
        await _queue.Writer.WriteAsync(message, ct);
        return message;
    }

    public IAsyncEnumerable<OutboxMessage> ReadAllAsync(CancellationToken ct) => _queue.Reader.ReadAllAsync(ct);

    public bool TryGet(string id, out OutboxMessage message) => _messages.TryGetValue(id, out message!);

    public void MarkProcessing(string id)
    {
        if (_messages.TryGetValue(id, out var existing))
        {
            _messages[id] = existing with
            {
                Status = JobStatus.Processing,
                StartedAtUtc = DateTimeOffset.UtcNow
            };
        }
    }

    public void MarkCompleted(string id, long executionMs, double rowsPerSecond)
    {
        if (_messages.TryGetValue(id, out var existing))
        {
            _messages[id] = existing with
            {
                Status = JobStatus.Completed,
                CompletedAtUtc = DateTimeOffset.UtcNow,
                ExecutionMs = executionMs,
                RowsPerSecond = rowsPerSecond,
                Error = null
            };
        }
    }

    public void MarkFailed(string id, string error)
    {
        if (_messages.TryGetValue(id, out var existing))
        {
            _messages[id] = existing with
            {
                Status = JobStatus.Failed,
                CompletedAtUtc = DateTimeOffset.UtcNow,
                Error = error
            };
        }
    }
}

sealed class FileStorage
{
    private readonly string _importsDirectory;

    public FileStorage(string rootPath)
    {
        _importsDirectory = Path.Combine(rootPath, "imports");
        Directory.CreateDirectory(_importsDirectory);
    }

    public async Task<SavedUpload> SaveUploadAsync(IFormFile file, CancellationToken ct)
    {
        var storedPath = Path.Combine(_importsDirectory, $"{Guid.NewGuid():n}.csv");

        using var input = file.OpenReadStream();
        await using var output = File.Create(storedPath);
        using var sha = SHA256.Create();

        var buffer = ArrayPool<byte>.Shared.Rent(80 * 1024);
        try
        {
            while (true)
            {
                var bytesRead = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct);
                if (bytesRead == 0)
                {
                    break;
                }

                sha.TransformBlock(buffer, 0, bytesRead, null, 0);
                await output.WriteAsync(buffer.AsMemory(0, bytesRead), ct);
            }

            sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        var hash = Convert.ToHexString(sha.Hash!).ToLowerInvariant();
        var safeName = Path.GetFileName(file.FileName);

        return new SavedUpload(safeName, hash, storedPath);
    }

    public string CreateDerivedCsvPath(string importId)
        => Path.Combine(_importsDirectory, $"{importId}-{Guid.NewGuid():n}.csv");

    public void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
        }
    }
}

sealed class ImportSession
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

static class CsvUtilities
{
    private static readonly Regex EmailRegex = new(@"^[^\s@]+@[^\s@]+\.[^\s@]+$", RegexOptions.Compiled);
    private static readonly Regex PhoneRegex = new(@"^[+\d][\d\s()-]{6,}$", RegexOptions.Compiled);

    public static async Task<CsvAnalysisResult> AnalyzeFileAsync(string filePath, int uniqueTrackingLimit, CancellationToken ct)
    {
        var started = DateTimeOffset.UtcNow;

        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        var headerLine = await reader.ReadLineAsync(ct);
        if (string.IsNullOrWhiteSpace(headerLine))
        {
            return new CsvAnalysisResult(new List<string>(), 0, new List<Dictionary<string, string>>(), new List<ColumnProfile>(), new List<Anomaly>(), new List<TransformRule>(), 1);
        }

        var headers = ParseLine(headerLine).Select(h => h.Trim()).ToList();
        var accumulators = headers.ToDictionary(
            h => h,
            h => new ColumnAccumulator(h, uniqueTrackingLimit),
            StringComparer.OrdinalIgnoreCase);

        var previewRows = new List<Dictionary<string, string>>();
        var rowCount = 0;
        var hasIdColumn = headers.Any(h => h.Equals("id", StringComparison.OrdinalIgnoreCase));
        var seenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var duplicateIdCount = 0;

        string? line;
        while ((line = await reader.ReadLineAsync(ct)) is not null)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = ParseLine(line);
            var row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < headers.Count; i++)
            {
                var value = i < values.Count ? values[i] : string.Empty;
                row[headers[i]] = value;
                accumulators[headers[i]].Observe(value);
            }

            if (hasIdColumn && row.TryGetValue("id", out var idValue) && !string.IsNullOrWhiteSpace(idValue) && !seenIds.Add(idValue))
            {
                duplicateIdCount++;
            }

            if (previewRows.Count < 20)
            {
                previewRows.Add(row);
            }

            rowCount++;
        }

        var profile = headers.Select(h => accumulators[h].ToProfile(rowCount)).ToList();
        var anomalies = headers.SelectMany(h => accumulators[h].ToAnomalies()).ToList();
        if (duplicateIdCount > 0)
        {
            anomalies.Add(new Anomaly("id", "duplicate_id", "ID column has duplicate values.", duplicateIdCount));
        }

        var suggestions = TransformSuggester.Suggest(headers, anomalies);
        var executionMs = Math.Max(1, (long)(DateTimeOffset.UtcNow - started).TotalMilliseconds);

        return new CsvAnalysisResult(headers, rowCount, previewRows, profile, anomalies, suggestions, executionMs);
    }

    public static async Task TransformFileAsync(string inputPath, string outputPath, List<TransformRule> rules, CancellationToken ct)
    {
        var enabledRules = rules.Where(r => r.Enabled).ToList();

        await using var inputStream = File.OpenRead(inputPath);
        using var reader = new StreamReader(inputStream, Encoding.UTF8);
        await using var outputStream = File.Create(outputPath);
        await using var writer = new StreamWriter(outputStream, Encoding.UTF8);

        var headerLine = await reader.ReadLineAsync(ct);
        if (string.IsNullOrWhiteSpace(headerLine))
        {
            return;
        }

        var sourceHeaders = ParseLine(headerLine).Select(h => h.Trim()).ToList();
        var outputHeaders = ApplyHeaderRules(sourceHeaders, enabledRules);

        await writer.WriteLineAsync(string.Join(',', outputHeaders.Select(Escape)));

        string? line;
        while ((line = await reader.ReadLineAsync(ct)) is not null)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = ParseLine(line);
            var row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < sourceHeaders.Count; i++)
            {
                row[sourceHeaders[i]] = i < values.Count ? values[i] : string.Empty;
            }

            ApplyRowRules(row, enabledRules);

            var orderedValues = outputHeaders.Select(h => row.TryGetValue(h, out var value) ? value ?? string.Empty : string.Empty);
            await writer.WriteLineAsync(string.Join(',', orderedValues.Select(Escape)));
        }

        await writer.FlushAsync(ct);
    }

    public static async Task WriteJsonAsync(string csvPath, Stream output, CancellationToken ct)
    {
        await using var stream = File.OpenRead(csvPath);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        await using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = true });

        var headerLine = await reader.ReadLineAsync(ct);
        var headers = string.IsNullOrWhiteSpace(headerLine)
            ? new List<string>()
            : ParseLine(headerLine).Select(h => h.Trim()).ToList();

        writer.WriteStartArray();

        string? line;
        while ((line = await reader.ReadLineAsync(ct)) is not null)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var values = ParseLine(line);
            writer.WriteStartObject();
            for (var i = 0; i < headers.Count; i++)
            {
                var value = i < values.Count ? values[i] : string.Empty;
                writer.WriteString(headers[i], value);
            }

            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        await writer.FlushAsync(ct);
    }

    private static List<string> ApplyHeaderRules(List<string> sourceHeaders, List<TransformRule> rules)
    {
        var headers = new List<string>(sourceHeaders);

        foreach (var rule in rules)
        {
            if (!rule.Operation.Equals("rename", StringComparison.OrdinalIgnoreCase) || string.IsNullOrWhiteSpace(rule.Parameter))
            {
                continue;
            }

            var index = headers.FindIndex(h => h.Equals(rule.Column, StringComparison.OrdinalIgnoreCase));
            if (index < 0 || headers.Any(h => h.Equals(rule.Parameter, StringComparison.OrdinalIgnoreCase)))
            {
                continue;
            }

            headers[index] = rule.Parameter;
        }

        return headers;
    }

    private static void ApplyRowRules(Dictionary<string, string> row, List<TransformRule> rules)
    {
        foreach (var rule in rules)
        {
            if (rule.Operation.Equals("rename", StringComparison.OrdinalIgnoreCase))
            {
                RenameColumn(row, rule.Column, rule.Parameter ?? string.Empty);
                continue;
            }

            var actualKey = row.Keys.FirstOrDefault(k => k.Equals(rule.Column, StringComparison.OrdinalIgnoreCase));
            if (actualKey is null)
            {
                continue;
            }

            var value = row.TryGetValue(actualKey, out var existing) ? existing ?? string.Empty : string.Empty;
            row[actualKey] = rule.Operation.ToLowerInvariant() switch
            {
                "trim" => value.Trim(),
                "tolower" => value.ToLowerInvariant(),
                "toupper" => value.ToUpperInvariant(),
                "nullifempty" => string.IsNullOrWhiteSpace(value) ? string.Empty : value,
                "normalizestatus" => NormalizeStatus(value),
                "normalizecity" => NormalizeCity(value),
                "normalizephone" => NormalizePhone(value),
                "parsenumber" => ParseNumber(value),
                "normalizedate" => NormalizeDate(value),
                _ => value
            };
        }
    }

    private static void RenameColumn(Dictionary<string, string> row, string oldName, string newName)
    {
        if (string.IsNullOrWhiteSpace(newName))
        {
            return;
        }

        var currentKey = row.Keys.FirstOrDefault(k => k.Equals(oldName, StringComparison.OrdinalIgnoreCase));
        if (currentKey is null || row.Keys.Any(k => k.Equals(newName, StringComparison.OrdinalIgnoreCase)))
        {
            return;
        }

        var value = row[currentKey];
        row.Remove(currentKey);
        row[newName] = value;
    }

    private static string Escape(string value)
    {
        if (value.Contains('"') || value.Contains(',') || value.Contains('\n') || value.Contains('\r'))
        {
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }

        return value;
    }

    private static List<string> ParseLine(string line)
    {
        var values = new List<string>();
        var current = new StringBuilder();
        var inQuotes = false;

        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];

            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    current.Append('"');
                    i++;
                    continue;
                }

                inQuotes = !inQuotes;
                continue;
            }

            if (c == ',' && !inQuotes)
            {
                values.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(c);
        }

        values.Add(current.ToString());
        return values;
    }

    private static string NormalizeStatus(string value)
    {
        var normalized = value.Trim().ToLowerInvariant();
        return normalized switch
        {
            "lost" => "Lost",
            "found" => "Found",
            _ => value.Trim()
        };
    }

    private static string NormalizeCity(string value)
    {
        var trimmed = value.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
        {
            return string.Empty;
        }

        return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(trimmed.ToLowerInvariant());
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
            return "+49" + trimmed[4..];
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
        if (DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dt))
        {
            return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        return DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dt)
            ? dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : string.Empty;
    }

    private static bool TryParseDate(string value, out DateTime date)
    {
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var unix) && unix > 1000000000)
        {
            try
            {
                date = DateTimeOffset.FromUnixTimeSeconds((long)unix).UtcDateTime;
                return true;
            }
            catch
            {
                date = default;
                return false;
            }
        }

        var formats = new[] { "yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "MM/dd/yyyy" };
        if (DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dt))
        {
            date = dt;
            return true;
        }

        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dt))
        {
            date = dt;
            return true;
        }

        date = default;
        return false;
    }

    private static bool IsKnownStatus(string value)
        => value.Trim().Equals("lost", StringComparison.OrdinalIgnoreCase)
        || value.Trim().Equals("found", StringComparison.OrdinalIgnoreCase);

    private sealed class ColumnAccumulator
    {
        private readonly string _header;
        private readonly int _uniqueTrackingLimit;
        private readonly HashSet<string> _uniqueValues = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _categories = new(StringComparer.OrdinalIgnoreCase);
        private readonly bool _emailColumn;
        private readonly bool _phoneColumn;
        private readonly bool _statusColumn;

        private bool _allInt = true;
        private bool _allFloat = true;
        private bool _allBool = true;
        private bool _allDate = true;
        private int _nonEmpty;
        private int _trimmedIssues;
        private int _invalidEmail;
        private int _invalidPhone;
        private int _invalidStatus;
        private int _maxLength;
        private double? _min;
        private double? _max;
        private bool _uniqueOverflow;

        public ColumnAccumulator(string header, int uniqueTrackingLimit)
        {
            _header = header;
            _uniqueTrackingLimit = uniqueTrackingLimit;
            _emailColumn = header.Contains("email", StringComparison.OrdinalIgnoreCase);
            _phoneColumn = header.Contains("phone", StringComparison.OrdinalIgnoreCase);
            _statusColumn = header.Contains("status", StringComparison.OrdinalIgnoreCase);
        }

        public void Observe(string value)
        {
            var current = value ?? string.Empty;
            if (string.IsNullOrWhiteSpace(current))
            {
                return;
            }

            _nonEmpty++;
            if (current != current.Trim())
            {
                _trimmedIssues++;
            }

            _maxLength = Math.Max(_maxLength, current.Length);

            if (!_uniqueOverflow)
            {
                if (_uniqueValues.Count < _uniqueTrackingLimit)
                {
                    _uniqueValues.Add(current);
                }
                else
                {
                    _uniqueOverflow = true;
                }
            }

            var trimmed = current.Trim();
            var category = "string";

            if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            {
                category = "int";
            }
            else
            {
                _allInt = false;
            }

            if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var num))
            {
                if (category == "string")
                {
                    category = "float";
                }

                _min = _min is null ? num : Math.Min(_min.Value, num);
                _max = _max is null ? num : Math.Max(_max.Value, num);
            }
            else
            {
                _allFloat = false;
            }

            if (bool.TryParse(trimmed, out _))
            {
                if (category == "string")
                {
                    category = "bool";
                }
            }
            else
            {
                _allBool = false;
            }

            if (TryParseDate(trimmed, out _))
            {
                if (category == "string")
                {
                    category = "date";
                }
            }
            else
            {
                _allDate = false;
            }

            _categories.Add(category);

            if (_emailColumn && !EmailRegex.IsMatch(trimmed))
            {
                _invalidEmail++;
            }

            if (_phoneColumn && !PhoneRegex.IsMatch(trimmed))
            {
                _invalidPhone++;
            }

            if (_statusColumn && !IsKnownStatus(trimmed))
            {
                _invalidStatus++;
            }
        }

        public ColumnProfile ToProfile(int totalRows)
        {
            var inferredType = InferType();
            return new ColumnProfile(
                _header,
                inferredType,
                totalRows,
                _nonEmpty,
                totalRows == 0 ? 0 : Math.Round((totalRows - _nonEmpty) / (double)totalRows, 3),
                _uniqueOverflow ? _uniqueValues.Count + 1 : _uniqueValues.Count,
                inferredType is "int" or "float" ? _min : null,
                inferredType is "int" or "float" ? _max : null,
                inferredType is "int" or "float" ? 0 : _maxLength,
                _uniqueValues.Take(3).ToList());
        }

        public List<Anomaly> ToAnomalies()
        {
            var result = new List<Anomaly>();

            if (_trimmedIssues > 0)
            {
                result.Add(new Anomaly(_header, "whitespace", "Values contain leading/trailing whitespace.", _trimmedIssues));
            }

            if (_categories.Count > 1)
            {
                result.Add(new Anomaly(_header, "mixed_types", "Column contains mixed value types.", _nonEmpty));
            }

            if (_invalidEmail > 0)
            {
                result.Add(new Anomaly(_header, "invalid_email", "Email format appears invalid.", _invalidEmail));
            }

            if (_invalidPhone > 0)
            {
                result.Add(new Anomaly(_header, "invalid_phone", "Phone format appears invalid.", _invalidPhone));
            }

            if (_invalidStatus > 0)
            {
                result.Add(new Anomaly(_header, "invalid_status", "Status should be Lost or Found.", _invalidStatus));
            }

            return result;
        }

        private string InferType()
        {
            if (_nonEmpty == 0)
            {
                return "string";
            }

            if (_allInt)
            {
                return "int";
            }

            if (_allFloat)
            {
                return "float";
            }

            if (_allBool)
            {
                return "bool";
            }

            if (_allDate)
            {
                return "date";
            }

            return "string";
        }
    }
}

static class TransformSuggester
{
    public static List<TransformRule> Suggest(List<string> headers, List<Anomaly> anomalies)
    {
        var result = new List<TransformRule>();

        foreach (var header in headers)
        {
            result.Add(new TransformRule("trim", header, null, true));

            if (header.Contains("email", StringComparison.OrdinalIgnoreCase))
            {
                result.Add(new TransformRule("toLower", header, null, true));
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
            .GroupBy(r => $"{r.Operation}|{r.Column}|{r.Parameter}", StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList();
    }
}

record ProcessingOptions(int WorkerCount, int QueueCapacity, int MaxUniqueTracking);
record SavedUpload(string OriginalFileName, string FileHash, string StoredPath);
record ImportSnapshot(int RowCount, List<string> Headers, List<Dictionary<string, string>> PreviewRows, List<ColumnProfile> Profile, List<Anomaly> Anomalies, List<TransformRule> SuggestedRules, long ExecutionMs, double RowsPerSecond);
record CsvAnalysisResult(List<string> Headers, int RowCount, List<Dictionary<string, string>> PreviewRows, List<ColumnProfile> Profile, List<Anomaly> Anomalies, List<TransformRule> SuggestedRules, long ExecutionMs);
record TransformRequest(List<TransformRule>? Rules);
record TransformRule(string Operation, string Column, string? Parameter, bool Enabled = true);
record ColumnProfile(string Column, string InferredType, int TotalRows, int NonEmptyRows, double NullRate, int UniqueCount, double? Min, double? Max, int MaxLength, List<string> Samples);
record Anomaly(string Column, string Code, string Message, int Count);
record OutboxMessage(string Id, string ImportId, JobType Type, List<TransformRule> Rules, JobStatus Status, DateTimeOffset CreatedAtUtc, DateTimeOffset? StartedAtUtc, DateTimeOffset? CompletedAtUtc, long? ExecutionMs, double? RowsPerSecond, string? Error);

enum JobType
{
    Profile,
    Transform
}

enum JobStatus
{
    Pending,
    Processing,
    Completed,
    Failed
}
