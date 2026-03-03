using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Wal;

namespace Lumina.Tests.Benchmarks;

/// <summary>
/// BenchmarkDotNet harness measuring WAL write throughput.
/// Target: ≥ 500 MB/s sustained ingestion.
///
/// Run from the command line with:
///   dotnet run -c Release --project Tests -- --filter *IngestionBenchmarks*
/// </summary>
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net90, iterationCount: 5, warmupCount: 2)]
[Config(typeof(Config))]
public class IngestionBenchmarks
{
  private sealed class Config : ManualConfig
  {
    public Config()
    {
      AddColumn(StatisticColumn.P95);
      AddColumn(new ThroughputColumn());
    }
  }

  private string _tempDir = null!;
  private WalSettings _settings = null!;
  private LogEntry[] _entries = null!;

  [Params(1_000, 10_000)]
  public int BatchSize { get; set; }

  [GlobalSetup]
  public void Setup()
  {
    _tempDir = Path.Combine(Path.GetTempPath(), "LuminaBench", Guid.NewGuid().ToString());
    Directory.CreateDirectory(_tempDir);

    _settings = new WalSettings {
      DataDirectory = _tempDir,
      MaxWalSizeBytes = 512 * 1024 * 1024, // 512 MB – avoid rotation during bench
      EnableWriteThrough = false,
      FlushIntervalMs = 0
    };

    _entries = Enumerable.Range(0, BatchSize).Select(i => new LogEntry {
      Stream = "bench-stream",
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = $"Benchmark log entry {i} with some realistic payload data for throughput measurement",
      Attributes = new Dictionary<string, object?> {
        ["host"] = "server-01",
        ["service"] = "api-gateway",
        ["region"] = "us-east-1",
        ["status_code"] = 200,
        ["latency_ms"] = 42
      }
    }).ToArray();
  }

  [GlobalCleanup]
  public void Cleanup()
  {
    if (Directory.Exists(_tempDir)) {
      try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
  }

  [Benchmark(Description = "WAL WriteBatchAsync")]
  public async Task<long[]> WalWriteBatch()
  {
    await using var walManager = new WalManager(_settings);
    var writer = await walManager.GetOrCreateWriterAsync("bench-stream");
    return await writer.WriteBatchAsync(_entries);
  }

  [Benchmark(Description = "WAL Sequential WriteAsync")]
  public async Task WalSequentialWrite()
  {
    await using var walManager = new WalManager(_settings);
    var writer = await walManager.GetOrCreateWriterAsync("bench-stream");
    foreach (var entry in _entries) {
      await writer.WriteAsync(entry);
    }
  }

  /// <summary>
  /// Custom column that computes MB/s throughput from the benchmark results.
  /// </summary>
  private sealed class ThroughputColumn : IColumn
  {
    public string Id => "Throughput";
    public string ColumnName => "MB/s (est)";
    public bool AlwaysShow => true;
    public ColumnCategory Category => ColumnCategory.Custom;
    public int PriorityInCategory => 0;
    public bool IsNumeric => true;
    public UnitType UnitType => UnitType.Dimensionless;
    public string Legend => "Estimated throughput in MB/s based on ~200 bytes per entry";

    public string GetValue(BenchmarkDotNet.Reports.Summary summary, BenchmarkDotNet.Running.BenchmarkCase benchmarkCase)
    {
      var report = summary[benchmarkCase];
      if (report?.ResultStatistics == null) return "N/A";

      var batchParam = benchmarkCase.Parameters["BatchSize"];
      if (batchParam == null) return "N/A";

      var batchSize = (int)batchParam;
      const double estimatedBytesPerEntry = 200.0;
      var totalBytes = batchSize * estimatedBytesPerEntry;
      var meanNs = report.ResultStatistics.Mean;
      var mbPerSec = totalBytes / (meanNs / 1_000_000_000.0) / (1024.0 * 1024.0);

      return mbPerSec.ToString("F1");
    }

    public string GetValue(BenchmarkDotNet.Reports.Summary summary, BenchmarkDotNet.Running.BenchmarkCase benchmarkCase, BenchmarkDotNet.Reports.SummaryStyle style)
        => GetValue(summary, benchmarkCase);

    public bool IsDefault(BenchmarkDotNet.Reports.Summary summary, BenchmarkDotNet.Running.BenchmarkCase benchmarkCase) => false;
    public bool IsAvailable(BenchmarkDotNet.Reports.Summary summary) => true;
  }
}
