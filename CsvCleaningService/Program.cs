using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<ImportStore>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapPost("/api/import/upload", async (HttpRequest request, ImportStore store) =>
{
    if (!request.HasFormContentType)
    {
        return Results.BadRequest(new { error = "Expected multipart/form-data." });
    }

    var form = await request.ReadFormAsync();
    var file = form.Files["file"] ?? form.Files.FirstOrDefault();
    if (file is null || file.Length == 0)
    {
        return Results.BadRequest(new { error = "CSV file is required." });
    }

    using var reader = new StreamReader(file.OpenReadStream(), Encoding.UTF8);
    var csvContent = await reader.ReadToEndAsync();
    var parsed = CsvUtilities.Parse(csvContent);

    if (parsed.Headers.Count == 0)
    {
        return Results.BadRequest(new { error = "CSV has no header row." });
    }

    var importId = Guid.NewGuid().ToString("n");
    var session = new ImportSession(importId, file.FileName, parsed.Headers, parsed.Rows);
    session.ApplyFromOriginal(Array.Empty<TransformRule>());
    store.Save(session);

    var response = ResponseFactory.BuildImportResponse(session);
    return Results.Ok(response);
});

app.MapPost("/api/import/{importId}/transform", (string importId, TransformRequest request, ImportStore store) =>
{
    if (!store.TryGet(importId, out var session))
    {
        return Results.NotFound(new { error = "Import session not found." });
    }

    var rules = (request.Rules ?? new List<TransformRule>()).Where(r => r.Enabled).ToList();
    session.ApplyFromOriginal(rules);

    var response = ResponseFactory.BuildImportResponse(session);
    return Results.Ok(response);
});

app.MapGet("/api/import/{importId}/download", (string importId, string? format, ImportStore store) =>
{
    if (!store.TryGet(importId, out var session))
    {
        return Results.NotFound(new { error = "Import session not found." });
    }

    var selectedFormat = (format ?? "csv").ToLowerInvariant();
    if (selectedFormat == "json")
    {
        var json = JsonSerializer.Serialize(session.CurrentRows, new JsonSerializerOptions { WriteIndented = true });
        return Results.File(Encoding.UTF8.GetBytes(json), "application/json", $"clean-{session.FileName}.json");
    }

    var csv = CsvUtilities.ToCsv(session.Headers, session.CurrentRows);
    return Results.File(Encoding.UTF8.GetBytes(csv), "text/csv", $"clean-{session.FileName}.csv");
});

app.Run();

static class ResponseFactory
{
    public static object BuildImportResponse(ImportSession session)
    {
        var profile = Profiler.BuildProfile(session.Headers, session.CurrentRows);
        var anomalies = Profiler.BuildAnomalies(session.Headers, session.CurrentRows, profile);
        var suggestions = TransformSuggester.Suggest(session.Headers, anomalies);

        var preview = session.CurrentRows
            .Take(20)
            .Select(row => session.Headers.ToDictionary(h => h, h => row.TryGetValue(h, out var value) ? value : string.Empty))
            .ToList();

        return new
        {
            importId = session.Id,
            fileName = session.FileName,
            rowCount = session.CurrentRows.Count,
            headers = session.Headers,
            previewRows = preview,
            profile,
            anomalies,
            suggestedRules = suggestions
        };
    }
}

sealed class ImportStore
{
    private readonly ConcurrentDictionary<string, ImportSession> _sessions = new();

    public void Save(ImportSession session) => _sessions[session.Id] = session;

    public bool TryGet(string id, out ImportSession session) => _sessions.TryGetValue(id, out session!);
}

sealed class ImportSession
{
    private readonly object _sync = new();

    public string Id { get; }
    public string FileName { get; }
    public List<string> Headers { get; private set; }
    public List<Dictionary<string, string>> OriginalRows { get; }
    public List<Dictionary<string, string>> CurrentRows { get; private set; }

    public ImportSession(string id, string fileName, List<string> headers, List<Dictionary<string, string>> originalRows)
    {
        Id = id;
        FileName = fileName;
        Headers = headers;
        OriginalRows = originalRows;
        CurrentRows = CloneRows(originalRows);
    }

    public void ApplyFromOriginal(IEnumerable<TransformRule> rules)
    {
        lock (_sync)
        {
            var workingHeaders = new List<string>(Headers);
            var workingRows = CloneRows(OriginalRows);

            foreach (var rule in rules)
            {
                TransformationEngine.Apply(workingHeaders, workingRows, rule);
            }

            Headers = workingHeaders;
            CurrentRows = workingRows;
        }
    }

    private static List<Dictionary<string, string>> CloneRows(List<Dictionary<string, string>> source)
        => source.Select(row => row.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)).ToList();
}

static class CsvUtilities
{
    public static CsvParseResult Parse(string csv)
    {
        var lines = ReadLines(csv);
        if (lines.Count == 0)
        {
            return new CsvParseResult(new List<string>(), new List<Dictionary<string, string>>());
        }

        var headers = ParseLine(lines[0]).Select(h => h.Trim()).ToList();
        var rows = new List<Dictionary<string, string>>();

        foreach (var line in lines.Skip(1))
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
            }

            rows.Add(row);
        }

        return new CsvParseResult(headers, rows);
    }

    public static string ToCsv(List<string> headers, List<Dictionary<string, string>> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine(string.Join(',', headers.Select(Escape)));

        foreach (var row in rows)
        {
            var values = headers.Select(h => row.TryGetValue(h, out var v) ? v ?? string.Empty : string.Empty);
            sb.AppendLine(string.Join(',', values.Select(Escape)));
        }

        return sb.ToString();
    }

    private static string Escape(string value)
    {
        if (value.Contains('"') || value.Contains(',') || value.Contains('\n') || value.Contains('\r'))
        {
            return $"\"{value.Replace("\"", "\"\"")}\"";
        }

        return value;
    }

    private static List<string> ReadLines(string csv)
    {
        var lines = new List<string>();
        using var reader = new StringReader(csv);
        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            lines.Add(line);
        }

        return lines;
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
}

static class Profiler
{
    private static readonly Regex EmailRegex = new(@"^[^\s@]+@[^\s@]+\.[^\s@]+$", RegexOptions.Compiled);
    private static readonly Regex PhoneRegex = new(@"^[+\d][\d\s()-]{6,}$", RegexOptions.Compiled);

    public static List<ColumnProfile> BuildProfile(List<string> headers, List<Dictionary<string, string>> rows)
    {
        var result = new List<ColumnProfile>();

        foreach (var header in headers)
        {
            var values = rows.Select(r => r.TryGetValue(header, out var v) ? v ?? string.Empty : string.Empty).ToList();
            var nonEmpty = values.Where(v => !string.IsNullOrWhiteSpace(v)).ToList();
            var uniqueCount = nonEmpty.Distinct(StringComparer.OrdinalIgnoreCase).Count();

            var inferredType = InferType(nonEmpty);
            double? min = null;
            double? max = null;
            var stringMaxLen = 0;

            if (inferredType is "int" or "float")
            {
                var nums = nonEmpty.Select(TryParseNumber).Where(x => x.HasValue).Select(x => x!.Value).ToList();
                if (nums.Count > 0)
                {
                    min = nums.Min();
                    max = nums.Max();
                }
            }
            else
            {
                stringMaxLen = nonEmpty.Select(v => v.Length).DefaultIfEmpty(0).Max();
            }

            result.Add(new ColumnProfile(
                header,
                inferredType,
                rows.Count,
                nonEmpty.Count,
                rows.Count == 0 ? 0 : Math.Round((rows.Count - nonEmpty.Count) / (double)rows.Count, 3),
                uniqueCount,
                min,
                max,
                stringMaxLen,
                nonEmpty.Take(3).ToList()
            ));
        }

        return result;
    }

    public static List<Anomaly> BuildAnomalies(List<string> headers, List<Dictionary<string, string>> rows, List<ColumnProfile> profile)
    {
        var result = new List<Anomaly>();

        foreach (var col in profile)
        {
            var values = rows.Select(r => r.TryGetValue(col.Column, out var v) ? v ?? string.Empty : string.Empty).ToList();
            var nonEmpty = values.Where(v => !string.IsNullOrWhiteSpace(v)).ToList();
            if (nonEmpty.Count == 0)
            {
                continue;
            }

            var trimmedIssues = nonEmpty.Count(v => v != v.Trim());
            if (trimmedIssues > 0)
            {
                result.Add(new Anomaly(col.Column, "whitespace", "Values contain leading/trailing whitespace.", trimmedIssues));
            }

            var categories = nonEmpty.Select(ClassifyValue).Distinct().Count();
            if (categories > 1)
            {
                result.Add(new Anomaly(col.Column, "mixed_types", "Column contains mixed value types.", nonEmpty.Count));
            }

            if (col.Column.Contains("email", StringComparison.OrdinalIgnoreCase))
            {
                var invalid = nonEmpty.Count(v => !EmailRegex.IsMatch(v));
                if (invalid > 0)
                {
                    result.Add(new Anomaly(col.Column, "invalid_email", "Email format appears invalid.", invalid));
                }
            }

            if (col.Column.Contains("phone", StringComparison.OrdinalIgnoreCase))
            {
                var invalid = nonEmpty.Count(v => !PhoneRegex.IsMatch(v));
                if (invalid > 0)
                {
                    result.Add(new Anomaly(col.Column, "invalid_phone", "Phone format appears invalid.", invalid));
                }
            }

            if (col.Column.Contains("status", StringComparison.OrdinalIgnoreCase))
            {
                var invalid = nonEmpty.Count(v => !IsKnownStatus(v));
                if (invalid > 0)
                {
                    result.Add(new Anomaly(col.Column, "invalid_status", "Status should be Lost or Found.", invalid));
                }
            }
        }

        if (headers.Contains("id", StringComparer.OrdinalIgnoreCase))
        {
            var ids = rows
                .Select(r => r.TryGetValue("id", out var v) ? v : string.Empty)
                .Where(v => !string.IsNullOrWhiteSpace(v))
                .ToList();
            var dupCount = ids.Count - ids.Distinct(StringComparer.OrdinalIgnoreCase).Count();
            if (dupCount > 0)
            {
                result.Add(new Anomaly("id", "duplicate_id", "ID column has duplicate values.", dupCount));
            }
        }

        return result;
    }

    private static string InferType(List<string> values)
    {
        if (values.Count == 0)
        {
            return "string";
        }

        if (values.All(v => long.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out _)))
        {
            return "int";
        }

        if (values.All(v => TryParseNumber(v).HasValue))
        {
            return "float";
        }

        if (values.All(v => bool.TryParse(v, out _)))
        {
            return "bool";
        }

        if (values.All(v => TryParseDate(v).HasValue))
        {
            return "date";
        }

        return "string";
    }

    private static string ClassifyValue(string value)
    {
        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
        {
            return "int";
        }

        if (TryParseNumber(value).HasValue)
        {
            return "float";
        }

        if (bool.TryParse(value, out _))
        {
            return "bool";
        }

        if (TryParseDate(value).HasValue)
        {
            return "date";
        }

        return "string";
    }

    private static double? TryParseNumber(string value)
    {
        if (string.Equals(value, "unknown", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static DateTime? TryParseDate(string value)
    {
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var unix) && unix > 1000000000)
        {
            try
            {
                return DateTimeOffset.FromUnixTimeSeconds((long)unix).UtcDateTime;
            }
            catch
            {
                return null;
            }
        }

        var formats = new[] { "yyyy-MM-dd", "dd/MM/yyyy", "MM-dd-yyyy", "MM/dd/yyyy" };
        if (DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dt))
        {
            return dt;
        }

        return DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out dt) ? dt : null;
    }

    private static bool IsKnownStatus(string value)
        => value.Trim().Equals("lost", StringComparison.OrdinalIgnoreCase) || value.Trim().Equals("found", StringComparison.OrdinalIgnoreCase);
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

            if (header.Contains("value", StringComparison.OrdinalIgnoreCase) || anomalies.Any(a => a.Column == header && a.Code == "mixed_types"))
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

static class TransformationEngine
{
    public static void Apply(List<string> headers, List<Dictionary<string, string>> rows, TransformRule rule)
    {
        if (string.IsNullOrWhiteSpace(rule.Operation) || string.IsNullOrWhiteSpace(rule.Column))
        {
            return;
        }

        if (!headers.Contains(rule.Column, StringComparer.OrdinalIgnoreCase))
        {
            return;
        }

        if (rule.Operation.Equals("rename", StringComparison.OrdinalIgnoreCase))
        {
            RenameColumn(headers, rows, rule.Column, rule.Parameter ?? string.Empty);
            return;
        }

        foreach (var row in rows)
        {
            var actualColumn = headers.First(h => h.Equals(rule.Column, StringComparison.OrdinalIgnoreCase));
            var value = row.TryGetValue(actualColumn, out var existing) ? existing ?? string.Empty : string.Empty;

            row[actualColumn] = rule.Operation.ToLowerInvariant() switch
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

    private static void RenameColumn(List<string> headers, List<Dictionary<string, string>> rows, string oldName, string newName)
    {
        if (string.IsNullOrWhiteSpace(newName) || headers.Contains(newName, StringComparer.OrdinalIgnoreCase))
        {
            return;
        }

        var index = headers.FindIndex(h => h.Equals(oldName, StringComparison.OrdinalIgnoreCase));
        if (index < 0)
        {
            return;
        }

        var actualName = headers[index];
        headers[index] = newName;

        foreach (var row in rows)
        {
            if (row.TryGetValue(actualName, out var value))
            {
                row.Remove(actualName);
                row[newName] = value;
            }
        }
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
}

record CsvParseResult(List<string> Headers, List<Dictionary<string, string>> Rows);
record TransformRequest(List<TransformRule>? Rules);
record TransformRule(string Operation, string Column, string? Parameter, bool Enabled = true);
record ColumnProfile(
    string Column,
    string InferredType,
    int TotalRows,
    int NonEmptyRows,
    double NullRate,
    int UniqueCount,
    double? Min,
    double? Max,
    int MaxLength,
    List<string> Samples);
record Anomaly(string Column, string Code, string Message, int Count);
