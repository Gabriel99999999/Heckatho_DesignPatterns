using System.Buffers;
using System.Security.Cryptography;
using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Infrastructure.Storage;

public sealed class FileStorage
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

        return new SavedUpload(
            Path.GetFileName(file.FileName),
            Convert.ToHexString(sha.Hash!).ToLowerInvariant(),
            storedPath);
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
