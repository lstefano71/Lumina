using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

using Lumina.Core.Models;
using Lumina.Storage.Parquet;

namespace Lumina.Tests.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, iterationCount: 5, warmupCount: 2)]
[Microsoft.VSDiagnostics.CPUUsageDiagnoser]
public class ParquetReadBenchmarks
{
    private string _tempDir = null!;
    private string _parquetPath = null!;

    [Params(1_000, 10_000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public async System.Threading.Tasks.Task Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LuminaBench", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_tempDir);
        _parquetPath = Path.Combine(_tempDir, "read-benchmark.parquet");

        var entries = Enumerable.Range(0, RowCount).Select(i => new LogEntry
        {
            Stream = "bench-stream",
            Timestamp = DateTime.UtcNow.AddMilliseconds(i),
            Level = i % 2 == 0 ? "info" : "warn",
            Message = $"Benchmark log entry {i}",
            TraceId = $"trace-{i}",
            SpanId = $"span-{i}",
            DurationMs = i,
            Attributes = new Dictionary<string, object?>
            {
                ["host"] = "server-01",
                ["service"] = "api-gateway",
                ["region"] = "us-east-1",
                ["status_code"] = 200,
                ["latency_ms"] = i % 100
            }
        }).ToArray();

        await ParquetWriter.WriteBatchAsync(entries, _parquetPath);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    [Benchmark(Description = "ParquetReader.ReadEntriesAsync")]
    public async System.Threading.Tasks.Task<int> ReadAllEntries()
    {
        var count = 0;
        await foreach (var _ in ParquetReader.ReadEntriesAsync(_parquetPath))
        {
            count++;
        }

        return count;
    }
}

