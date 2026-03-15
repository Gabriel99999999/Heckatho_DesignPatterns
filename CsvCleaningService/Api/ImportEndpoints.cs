using CsvCleaningService.Application;
using CsvCleaningService.Domain.Entities;
using CsvCleaningService.Domain.Models;
using CsvCleaningService.Evaluation;
using CsvCleaningService.Infrastructure.Csv;
using CsvCleaningService.Infrastructure.Storage;
using CsvCleaningService.Infrastructure.Stores;

namespace CsvCleaningService.Api;

public static class ImportEndpoints
{
    public static void MapImportEndpoints(this WebApplication app)
    {
        app.MapPost("/api/import/upload", UploadAsync);
        app.MapPost("/api/import/{importId}/transform", QueueTransformAsync);
        app.MapGet("/api/import/{importId}", GetImport);
        app.MapGet("/api/job/{jobId}", GetJob);
        app.MapGet("/api/import/{importId}/download", DownloadAsync);
        app.MapPost("/api/eval/run", RunEvalAsync);
    }

    private static async Task<IResult> UploadAsync(HttpRequest request, ImportStore store, OutboxStore outbox, FileStorage storage, CancellationToken ct)
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
                return Results.Ok(ImportResponseFactory.BuildImportResponse(existing, true));
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

        var message = await outbox.EnqueueAsync(importId, JobType.Profile, [], ct);
        session.SetLatestJobId(message.Id);

        return Results.Accepted($"/api/import/{importId}", new
        {
            importId,
            fileName = upload.OriginalFileName,
            deduplicated = false,
            status = "queued",
            jobId = message.Id
        });
    }

    private static async Task<IResult> QueueTransformAsync(string importId, TransformRequest request, ImportStore store, OutboxStore outbox, Application.TransformRuleValidator validator, CancellationToken ct)
    {
        if (!store.TryGet(importId, out var session))
        {
            return Results.NotFound(new { error = "Import session not found." });
        }

        var rules = validator.Validate((request.Rules ?? []).Where(rule => rule.Enabled), session.Snapshot?.Headers ?? []);
        var message = await outbox.EnqueueAsync(importId, JobType.Transform, rules, ct);
        session.SetLatestJobId(message.Id);

        return Results.Accepted($"/api/import/{importId}", new
        {
            importId,
            status = "queued",
            jobId = message.Id
        });
    }

    private static IResult GetImport(string importId, ImportStore store)
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

        return Results.Ok(ImportResponseFactory.BuildImportResponse(session, false));
    }

    private static IResult GetJob(string jobId, OutboxStore outbox)
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
    }

    private static IResult DownloadAsync(string importId, string? format, ImportStore store, CsvFileProcessor csvFileProcessor, CancellationToken ct)
    {
        if (!store.TryGet(importId, out var session) || session.Snapshot is null)
        {
            return Results.NotFound(new { error = "Import session not found or not ready." });
        }

        var selectedFormat = (format ?? "csv").ToLowerInvariant();
        var fileName = BuildCleanName(session.FileName);

        if (selectedFormat == "json")
        {
            return Results.Stream(output => csvFileProcessor.WriteJsonAsync(session.CurrentFilePath, output, ct), "application/json", $"{fileName}.json");
        }

        return Results.File(session.CurrentFilePath, "text/csv", $"{fileName}.csv");
    }

    private static async Task<IResult> RunEvalAsync(EvalHarnessService evalHarnessService, IWebHostEnvironment environment, CancellationToken ct)
    {
        var result = await evalHarnessService.RunAsync(environment.ContentRootPath, ct);
        return Results.Ok(result);
    }

    private static string BuildCleanName(string original)
    {
        var name = Path.GetFileNameWithoutExtension(original);
        return $"clean-{(string.IsNullOrWhiteSpace(name) ? "cleaned" : name)}";
    }
}
