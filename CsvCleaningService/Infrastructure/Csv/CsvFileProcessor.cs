using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using CsvCleaningService.Application;
using CsvCleaningService.Domain.Models;

namespace CsvCleaningService.Infrastructure.Csv;

public sealed class CsvFileProcessor
{
    private readonly ContactValueValidator _validator;
    private readonly AiMappingSuggestionService _aiMappingSuggestionService;
    private readonly SandboxedTransformPipeline _pipeline;

    public CsvFileProcessor(
        ContactValueValidator validator,
        AiMappingSuggestionService aiMappingSuggestionService,
        SandboxedTransformPipeline pipeline)
    {
        _validator = validator;
        _aiMappingSuggestionService = aiMappingSuggestionService;
        _pipeline = pipeline;
    }

    public async Task<CsvAnalysisResult> AnalyzeFileAsync(string filePath, int uniqueTrackingLimit, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        var benchmark = new BenchmarkSampler();

        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        var headerLine = await reader.ReadLineAsync(ct);
        if (string.IsNullOrWhiteSpace(headerLine))
        {
            return new CsvAnalysisResult([], 0, [], [], [], [], benchmark.Build(1, 0), "local-heuristic-llm");
        }

        var headers = ParseLine(headerLine).Select(h => h.Trim()).ToList();
        var accumulators = headers.ToDictionary(
            header => header,
            header => new ColumnAccumulator(header, uniqueTrackingLimit, _validator),
            StringComparer.OrdinalIgnoreCase);

        var previewRows = new List<Dictionary<string, string>>();
        var rowCount = 0;
        var hasIdColumn = headers.Any(h => h.Equals("id", StringComparison.OrdinalIgnoreCase));
        var seenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var duplicateIdCount = 0;

        string? line;
        while ((line = await reader.ReadLineAsync(ct)) is not null)
        {
            var rowWatch = Stopwatch.StartNew();

            if (!string.IsNullOrWhiteSpace(line))
            {
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

            benchmark.CaptureRow(rowWatch.Elapsed.TotalMilliseconds);
        }

        var profile = headers.Select(h => accumulators[h].ToProfile(rowCount)).ToList();
        var anomalies = headers.SelectMany(h => accumulators[h].ToAnomalies()).ToList();
        if (duplicateIdCount > 0)
        {
            anomalies.Add(new Anomaly("id", "duplicate_id", "ID column has duplicate values.", duplicateIdCount));
        }

        var (suggestions, provider) = _aiMappingSuggestionService.Suggest(headers, profile, anomalies);
        stopwatch.Stop();

        return new CsvAnalysisResult(
            headers,
            rowCount,
            previewRows,
            profile,
            anomalies,
            suggestions,
            benchmark.Build(stopwatch.ElapsedMilliseconds, rowCount),
            provider);
    }

    public async Task<BenchmarkMetrics> TransformFileAsync(string inputPath, string outputPath, List<TransformRule> rules, CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        var benchmark = new BenchmarkSampler();

        await using var inputStream = File.OpenRead(inputPath);
        using var reader = new StreamReader(inputStream, Encoding.UTF8);
        await using var outputStream = File.Create(outputPath);
        await using var writer = new StreamWriter(outputStream, Encoding.UTF8);

        var headerLine = await reader.ReadLineAsync(ct);
        if (string.IsNullOrWhiteSpace(headerLine))
        {
            return benchmark.Build(1, 0);
        }

        var sourceHeaders = ParseLine(headerLine).Select(h => h.Trim()).ToList();
        var enabledRules = _pipeline.ValidateRules(rules.Where(r => r.Enabled), sourceHeaders);
        var outputHeaders = ApplyHeaderRules(sourceHeaders, enabledRules);
        await writer.WriteLineAsync(string.Join(',', outputHeaders.Select(Escape)));

        var rowCount = 0;
        string? line;
        while ((line = await reader.ReadLineAsync(ct)) is not null)
        {
            var rowWatch = Stopwatch.StartNew();

            if (!string.IsNullOrWhiteSpace(line))
            {
                var values = ParseLine(line);
                var row = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                for (var i = 0; i < sourceHeaders.Count; i++)
                {
                    row[sourceHeaders[i]] = i < values.Count ? values[i] : string.Empty;
                }

                ApplyRowRules(row, enabledRules);

                var orderedValues = outputHeaders.Select(h => row.TryGetValue(h, out var value) ? value ?? string.Empty : string.Empty);
                await writer.WriteLineAsync(string.Join(',', orderedValues.Select(Escape)));
                rowCount++;
            }

            benchmark.CaptureRow(rowWatch.Elapsed.TotalMilliseconds);
        }

        await writer.FlushAsync(ct);
        stopwatch.Stop();
        return benchmark.Build(stopwatch.ElapsedMilliseconds, rowCount);
    }

    public async Task WriteJsonAsync(string csvPath, Stream output, CancellationToken ct)
    {
        await using var stream = File.OpenRead(csvPath);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        await using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = true });

        var headerLine = await reader.ReadLineAsync(ct);
        var headers = string.IsNullOrWhiteSpace(headerLine) ? [] : ParseLine(headerLine).Select(h => h.Trim()).ToList();

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
                writer.WriteString(headers[i], i < values.Count ? values[i] : string.Empty);
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

    private void ApplyRowRules(Dictionary<string, string> row, List<TransformRule> rules)
    {
        foreach (var rule in rules)
        {
            if (rule.Operation.Equals("rename", StringComparison.OrdinalIgnoreCase))
            {
                RenameColumn(row, rule.Column, rule.Parameter ?? string.Empty);
                continue;
            }

            var actualKey = row.Keys.FirstOrDefault(key => key.Equals(rule.Column, StringComparison.OrdinalIgnoreCase));
            if (actualKey is null)
            {
                continue;
            }

            var value = row.TryGetValue(actualKey, out var existing) ? existing ?? string.Empty : string.Empty;
            row[actualKey] = _pipeline.Apply(rule, value);
        }
    }

    private static void RenameColumn(Dictionary<string, string> row, string oldName, string newName)
    {
        if (string.IsNullOrWhiteSpace(newName))
        {
            return;
        }

        var currentKey = row.Keys.FirstOrDefault(key => key.Equals(oldName, StringComparison.OrdinalIgnoreCase));
        if (currentKey is null || row.Keys.Any(key => key.Equals(newName, StringComparison.OrdinalIgnoreCase)))
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
            var currentChar = line[i];
            if (currentChar == '"')
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

            if (currentChar == ',' && !inQuotes)
            {
                values.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(currentChar);
        }

        values.Add(current.ToString());
        return values;
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
        if (DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var exactDate))
        {
            date = exactDate;
            return true;
        }

        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsedDate))
        {
            date = parsedDate;
            return true;
        }

        date = default;
        return false;
    }

    private sealed class BenchmarkSampler
    {
        private readonly List<double> _latenciesMs = [];
        private long _peakMemoryBytes = GC.GetTotalMemory(false);

        public void CaptureRow(double latencyMs)
        {
            _latenciesMs.Add(latencyMs);
            _peakMemoryBytes = Math.Max(_peakMemoryBytes, GC.GetTotalMemory(false));
        }

        public BenchmarkMetrics Build(long executionMs, int rowCount)
        {
            _latenciesMs.Sort();

            var p50 = Percentile(50);
            var p95 = Percentile(95);
            var max = _latenciesMs.Count == 0 ? 0d : _latenciesMs[^1];
            var rowsPerSecond = rowCount <= 0 ? 0d : Math.Round(rowCount / (Math.Max(1, executionMs) / 1000d), 2);

            return new BenchmarkMetrics(
                Math.Max(1, executionMs),
                rowsPerSecond,
                _peakMemoryBytes,
                new LatencySummary(Math.Round(p50, 3), Math.Round(p95, 3), Math.Round(max, 3), _latenciesMs.Count));
        }

        private double Percentile(int percentile)
        {
            if (_latenciesMs.Count == 0)
            {
                return 0d;
            }

            var index = (int)Math.Ceiling((_latenciesMs.Count * percentile) / 100d) - 1;
            index = Math.Clamp(index, 0, _latenciesMs.Count - 1);
            return _latenciesMs[index];
        }
    }

    private sealed class ColumnAccumulator
    {
        private readonly string _header;
        private readonly int _uniqueTrackingLimit;
        private readonly ContactValueValidator _validator;
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

        public ColumnAccumulator(string header, int uniqueTrackingLimit, ContactValueValidator validator)
        {
            _header = header;
            _uniqueTrackingLimit = uniqueTrackingLimit;
            _validator = validator;
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

            if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var numericValue))
            {
                if (category == "string")
                {
                    category = "float";
                }

                _min = _min is null ? numericValue : Math.Min(_min.Value, numericValue);
                _max = _max is null ? numericValue : Math.Max(_max.Value, numericValue);
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

            if (_emailColumn && !_validator.IsValidEmail(trimmed))
            {
                _invalidEmail++;
            }

            if (_phoneColumn && !_validator.IsValidPhone(trimmed))
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

        private static bool IsKnownStatus(string value)
            => value.Equals("lost", StringComparison.OrdinalIgnoreCase)
            || value.Equals("found", StringComparison.OrdinalIgnoreCase);
    }
}
