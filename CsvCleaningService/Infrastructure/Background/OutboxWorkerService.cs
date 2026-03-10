using CsvCleaningService.Domain.Models;
using CsvCleaningService.Infrastructure.Csv;
using CsvCleaningService.Infrastructure.Stores;

namespace CsvCleaningService.Infrastructure.Background;

public sealed class OutboxWorkerService : BackgroundService
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
