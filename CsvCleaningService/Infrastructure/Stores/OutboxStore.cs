using System.Collections.Concurrent;
using System.Threading.Channels;
using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Infrastructure.Stores;

public sealed class OutboxStore
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
            _messages[id] = existing with { Status = JobStatus.Processing, StartedAtUtc = DateTimeOffset.UtcNow };
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
            _messages[id] = existing with { Status = JobStatus.Failed, CompletedAtUtc = DateTimeOffset.UtcNow, Error = error };
        }
    }
}
