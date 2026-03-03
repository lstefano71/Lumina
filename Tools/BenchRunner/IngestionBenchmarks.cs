using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Serialization;
using Lumina.Storage.Wal;

namespace Lumina.Tests.Benchmarks;

[MemoryDiagnoser]
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
  // FIX 2: created once in GlobalSetup and reused across all iterations so that
  // FileStream open / WAL-header-write overhead is NOT included in the measurement.
  private WalManager _walManager = null!;
  private WalWriter _writer = null!;

  [Params(1_000, 10_000)]
  public int BatchSize { get; set; }

  [GlobalSetup]
  public async Task Setup()
  {
    _tempDir = Path.Combine(Path.GetTempPath(), "LuminaBench", Guid.NewGuid().ToString());
    Directory.CreateDirectory(_tempDir);

    _settings = new WalSettings {
      DataDirectory = _tempDir,
      MaxWalSizeBytes = 512 * 1024 * 1024, // 512 MB – avoid rotation during bench
      // FIX 1: use the production default so we measure durable (write-through) I/O.
      EnableWriteThrough = true,
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

    // Create the writer once; subsequent benchmark iterations just append to the
    // already-open FileStream, which is the steady-state production behaviour.
    _walManager = new WalManager(_settings);
    _writer = await _walManager.GetOrCreateWriterAsync("bench-stream");

  }

  [GlobalCleanup]
  public async Task Cleanup()
  {
    await _walManager.DisposeAsync();
    if (Directory.Exists(_tempDir)) {
      try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }
  }

  [Benchmark(Description = "WAL WriteBatchAsync")]
  public async Task<long[]> WalWriteBatch() =>
      await _writer.WriteBatchAsync(_entries);

  [Benchmark(Description = "WAL Sequential WriteAsync")]
  public async Task WalSequentialWrite()
  {
    foreach (var entry in _entries) {
      await _writer.WriteAsync(entry);
    }
  }

  private sealed class ThroughputColumn : IColumn
  {
    // FIX 3: compute actual on-disk bytes PER ENTRY in the host process (where this column
    // runs) rather than in the benchmark sub-process.  BenchmarkDotNet spawns a new process
    // for every benchmark case, so any static field set inside GlobalSetup is invisible here.
    private static readonly int ActualBytesPerEntry = ComputeBytesPerEntry();

    private static int ComputeBytesPerEntry()
    {
      var sample = new LogEntry {
        Stream = "bench-stream",
        Timestamp = DateTime.UtcNow,
        Level = "info",
        Message = "Benchmark log entry 0 with some realistic payload data for throughput measurement",
        Attributes = new Dictionary<string, object?> {
          ["host"] = "server-01",
          ["service"] = "api-gateway",
          ["region"] = "us-east-1",
          ["status_code"] = 200,
          ["latency_ms"] = 42
        }
      };
      var payload = LogEntrySerializer.Serialize(sample);
      return WalFrameHeader.Size + payload.Length;
    }

    public string Id => "Throughput";
    public string ColumnName => "MB/s";
    public bool AlwaysShow => true;
    public ColumnCategory Category => ColumnCategory.Custom;
    public int PriorityInCategory => 0;
    public bool IsNumeric => true;
    public UnitType UnitType => UnitType.Dimensionless;
    public string Legend => $"Actual throughput in MB/s based on {ActualBytesPerEntry} bytes/entry (frame header + MessagePack payload)";

    public string GetValue(BenchmarkDotNet.Reports.Summary summary, BenchmarkDotNet.Running.BenchmarkCase benchmarkCase)
    {
      var report = summary[benchmarkCase];
      if (report?.ResultStatistics == null) return "N/A";

      var batchParam = benchmarkCase.Parameters["BatchSize"];
      if (batchParam == null) return "N/A";

      var batchSize = (int)batchParam;
      var totalBytes = (double)batchSize * ActualBytesPerEntry;
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
