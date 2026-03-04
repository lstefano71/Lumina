// Lumina - High-performance, append-only log storage and retrieval engine
// for observability and event streaming.

using Lumina.Core.Configuration;
using Lumina.Ingestion.Endpoints;
using Lumina.Observability;
using Lumina.Query;
using Lumina.Query.Endpoints;
using Lumina.Storage.Catalog;
using Lumina.Storage.Compaction;
using Lumina.Storage.Wal;

using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

// Bind configuration
builder.Services.Configure<LuminaSettings>(
    builder.Configuration.GetSection("Lumina"));

// Get settings for Kestrel configuration
var luminaSettings = new LuminaSettings();
builder.Configuration.GetSection("Lumina").Bind(luminaSettings);

// Configure Kestrel for HTTP/2 Cleartext (h2c) and HTTP/1.1
builder.WebHost.ConfigureKestrel(options => {
  options.ListenAnyIP(luminaSettings.Ingestion.HttpPort, listenOptions => {
    // Enable HTTP/2 Cleartext (h2c) for high-throughput ingestion
    listenOptions.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http1AndHttp2;
  });

  // Configure limits for high-throughput scenarios
  options.Limits.MaxRequestBodySize = luminaSettings.Ingestion.MaxRequestBodySize;
  options.Limits.MinRequestBodyDataRate = null;
  options.Limits.MinResponseDataRate = null;
});

// Add services
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Register configuration as singletons
builder.Services.AddSingleton(luminaSettings.Wal);
builder.Services.AddSingleton(luminaSettings.Compaction);
builder.Services.AddSingleton(luminaSettings.Query);

// Register core services
builder.Services.AddSingleton<WalManager>(sp => {
  var settings = sp.GetRequiredService<WalSettings>();
  return new WalManager(settings);
});

// Register cursor validation and recovery services
builder.Services.AddSingleton<CursorValidator>();
builder.Services.AddSingleton<CursorRecoveryService>(sp => {
  var walManager = sp.GetRequiredService<WalManager>();
  var settings = sp.GetRequiredService<CompactionSettings>();
  var logger = sp.GetRequiredService<ILogger<CursorRecoveryService>>();
  return new CursorRecoveryService(walManager, settings, logger);
});

builder.Services.AddSingleton<CursorManager>(sp => {
  var settings = sp.GetRequiredService<CompactionSettings>();
  var validator = sp.GetRequiredService<CursorValidator>();
  var recoveryService = sp.GetRequiredService<CursorRecoveryService>();
  var logger = sp.GetRequiredService<ILogger<CursorManager>>();
  return new CursorManager(
      settings.CursorDirectory,
      validator,
      recoveryService,
      logger,
      settings.EnableCursorValidation,
      settings.EnableCursorRecovery);
});

// Register catalog services
builder.Services.AddSingleton<CatalogOptions>(sp => {
  var settings = sp.GetRequiredService<CompactionSettings>();
  return new CatalogOptions {
    CatalogDirectory = settings.CatalogDirectory,
    EnableAutoRebuild = settings.EnableCatalogAutoRebuild,
    EnableStartupGc = settings.EnableCatalogStartupGc
  };
});

builder.Services.AddSingleton<CatalogManager>();
builder.Services.AddSingleton<CatalogRebuilder>();
builder.Services.AddSingleton<CatalogGarbageCollector>();

builder.Services.AddSingleton<L1Compactor>(sp => {
  var walManager = sp.GetRequiredService<WalManager>();
  var cursorManager = sp.GetRequiredService<CursorManager>();
  var settings = sp.GetRequiredService<CompactionSettings>();
  var logger = sp.GetRequiredService<ILogger<L1Compactor>>();
  var catalogManager = sp.GetRequiredService<CatalogManager>();
  return new L1Compactor(walManager, cursorManager, settings, logger, catalogManager);
});

builder.Services.AddSingleton<L2Compactor>(sp => {
  var settings = sp.GetRequiredService<CompactionSettings>();
  var parquetManager = sp.GetRequiredService<ParquetManager>();
  var logger = sp.GetRequiredService<ILogger<L2Compactor>>();
  var catalogManager = sp.GetRequiredService<CatalogManager>();
  return new L2Compactor(settings, parquetManager, logger, catalogManager);
});

builder.Services.AddSingleton<ParquetManager>(sp => {
  var settings = sp.GetRequiredService<CompactionSettings>();
  var logger = sp.GetRequiredService<ILogger<ParquetManager>>();
  var catalogManager = sp.GetRequiredService<CatalogManager>();
  return new ParquetManager(settings, logger, catalogManager);
});

builder.Services.AddSingleton<DuckDbQueryService>(sp => {
  var settings = sp.GetRequiredService<QuerySettings>();
  var parquetManager = sp.GetRequiredService<ParquetManager>();
  var logger = sp.GetRequiredService<ILogger<DuckDbQueryService>>();
  return new DuckDbQueryService(settings, parquetManager, logger);
});

// Register observability
builder.Services.AddSingleton<LuminaMetrics>();
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => {
      metrics.AddMeter(LuminaMetrics.MeterName);
      metrics.AddPrometheusExporter();
    });

// Register hosted services
builder.Services.AddHostedService<CompactorService>();
builder.Services.AddHostedService<StreamDiscoveryService>();

var app = builder.Build();

// Initialize catalog
var catalogManager = app.Services.GetRequiredService<CatalogManager>();
var catalogOptions = app.Services.GetRequiredService<CatalogOptions>();
var compactionSettings = app.Services.GetRequiredService<CompactionSettings>();
var catalogRebuilder = app.Services.GetRequiredService<CatalogRebuilder>();
var catalogGc = app.Services.GetRequiredService<CatalogGarbageCollector>();

await catalogManager.InitializeAsync();

// Check if catalog is empty and needs rebuilding
var catalogSnapshot = catalogManager.GetCatalogSnapshot();
if (catalogSnapshot.Entries.Count == 0 &&
    (Directory.Exists(compactionSettings.L1Directory) || Directory.Exists(compactionSettings.L2Directory))) {
  var logger = app.Services.GetRequiredService<ILogger<Program>>();
  logger.LogInformation("Catalog is empty, attempting rebuild from disk");

  var rebuiltCatalog = await catalogRebuilder.RecoverFromDiskAsync(
      compactionSettings.L1Directory,
      compactionSettings.L2Directory);

  if (rebuiltCatalog.Entries.Count > 0) {
    await catalogManager.ReloadFromStateAsync(rebuiltCatalog);
    logger.LogInformation("Catalog rebuilt with {Count} entries", rebuiltCatalog.Entries.Count);
  }
}

// Run startup garbage collection if enabled
if (catalogOptions.EnableStartupGc) {
  catalogSnapshot = catalogManager.GetCatalogSnapshot();
  await catalogGc.RunGcAsync(
      catalogSnapshot,
      compactionSettings.L1Directory,
      compactionSettings.L2Directory);
}

// Initialize DuckDB
var queryService = app.Services.GetRequiredService<DuckDbQueryService>();
await queryService.InitializeAsync();

// Configure Swagger in development
if (app.Environment.IsDevelopment()) {
  app.UseSwagger();
  app.UseSwaggerUI(options => {
    options.SwaggerEndpoint("/swagger/v1/swagger.json", "Lumina API v1");
  });
}

// Prometheus metrics endpoint
app.MapPrometheusScrapingEndpoint("/metrics");

// Map ingestion endpoints
JsonIngestionEndpoint.MapEndpoints(app);

// Map OTLP endpoints
app.MapOtlpEndpoints();

// Map query endpoints
app.MapQueryEndpoints();

// Health check endpoint
app.MapGet("/health", () => Results.Ok(new {
  status = "healthy",
  timestamp = DateTime.UtcNow,
  version = typeof(Program).Assembly.GetName().Version?.ToString() ?? "1.0.0"
}));

// Ready endpoint for orchestration
app.MapGet("/ready", () => Results.Ok(new {
  ready = true,
  timestamp = DateTime.UtcNow
}));

app.Run();