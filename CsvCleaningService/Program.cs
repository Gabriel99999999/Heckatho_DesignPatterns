using CsvCleaningService.Api;
using CsvCleaningService.Application;
using CsvCleaningService.Domain.Models;
using CsvCleaningService.Infrastructure.Background;
using CsvCleaningService.Infrastructure.Csv;
using CsvCleaningService.Infrastructure.Storage;
using CsvCleaningService.Infrastructure.Stores;

var builder = WebApplication.CreateBuilder(args);

var storageRoot = Path.Combine(builder.Environment.ContentRootPath, "App_Data");
Directory.CreateDirectory(storageRoot);

builder.Services.AddSingleton(new ProcessingOptions(Math.Max(2, Environment.ProcessorCount / 2), 64, 50_000));
builder.Services.AddSingleton(new FileStorage(storageRoot));
builder.Services.AddSingleton<ImportStore>();
builder.Services.AddSingleton<OutboxStore>();
builder.Services.AddSingleton<ContactValueValidator>();
builder.Services.AddSingleton<TransformSuggestionService>();
builder.Services.AddSingleton<CsvFileProcessor>();
builder.Services.AddSingleton<CsvProcessingService>();
builder.Services.AddSingleton<ImportResponseFactory>();
builder.Services.AddHostedService<OutboxWorkerService>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();
app.MapImportEndpoints();

app.Run();
