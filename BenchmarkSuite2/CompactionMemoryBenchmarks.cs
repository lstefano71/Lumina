using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Catalog;
using Lumina.Storage.Compaction;
using Lumina.Storage.Parquet;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VSDiagnostics;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Lumina.Tests.Benchmarks;
[SimpleJob(RuntimeMoniker.Net10_0, iterationCount: 3, warmupCount: 1)]
[CPUUsageDiagnoser]
public class CompactionMemoryBenchmarks
{
    private string _tempDir = null!;
    private string _l1Directory = null!;
    private string _l2Directory = null!;
    private string _catalogDirectory = null!;
    private CompactionSettings _settings = null!;
    private CatalogManager _catalogManager = null!;
    private CompactionPipeline _pipeline = null!;
    [Params(100)]
    public int SourceFileCount { get; set; }

    [Params(200)]
    public int RowsPerFile { get; set; }

    [IterationSetup]
    public async System.Threading.Tasks.Task SetupIteration()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LuminaBench", "compaction", Guid.NewGuid().ToString("N"));
        _l1Directory = Path.Combine(_tempDir, "l1");
        _l2Directory = Path.Combine(_tempDir, "l2");
        _catalogDirectory = Path.Combine(_tempDir, "catalog");
        Directory.CreateDirectory(_l1Directory);
        Directory.CreateDirectory(_l2Directory);
        Directory.CreateDirectory(_catalogDirectory);
        _settings = new CompactionSettings
        {
            L1Directory = _l1Directory,
            L2Directory = _l2Directory,
            CatalogDirectory = _catalogDirectory,
            MaxEntriesPerFile = 10000,
            L2IntervalHours = 0
        };
        var catalogOptions = new CatalogOptions
        {
            CatalogDirectory = _catalogDirectory,
            EnableAutoRebuild = false,
            EnableStartupGc = false
        };
        _catalogManager = new CatalogManager(catalogOptions, NullLogger<CatalogManager>.Instance);
        await _catalogManager.InitializeAsync();
        _pipeline = new CompactionPipeline(_settings, _catalogManager, new ICompactionTier[] { new DailyCompactionTier() }, NullLogger<CompactionPipeline>.Instance);
        var stream = "bench-stream";
        var day = DateTime.UtcNow.Date.AddDays(-2).AddHours(1);
        var streamDir = Path.Combine(_l1Directory, stream);
        Directory.CreateDirectory(streamDir);
        for (int fileIdx = 0; fileIdx < SourceFileCount; fileIdx++)
        {
            var fileStart = day.AddMinutes(fileIdx * 5);
            var entries = Enumerable.Range(0, RowsPerFile).Select(i => new LogEntry { Stream = stream, Timestamp = fileStart.AddSeconds(i), Level = i % 2 == 0 ? "info" : "warn", Message = $"msg-{fileIdx}-{i}", Attributes = new Dictionary<string, object?> { ["host"] = "server-01", ["status"] = 200, ["latency"] = i % 100 } }).ToArray();
            var filePath = Path.Combine(streamDir, $"{stream}_{fileStart:yyyyMMdd_HHmmss}_{fileStart.AddSeconds(RowsPerFile):yyyyMMdd_HHmmss}_{fileIdx}.parquet");
            await ParquetWriter.WriteBatchAsync(entries, filePath, 256);
            await _catalogManager.AddFileAsync(new CatalogEntry { StreamName = stream, MinTime = entries.Min(e => e.Timestamp), MaxTime = entries.Max(e => e.Timestamp), FilePath = filePath, Level = StorageLevel.L1, RowCount = entries.Length, FileSizeBytes = new FileInfo(filePath).Length, AddedAt = DateTime.UtcNow, CompactionTier = 1 });
        }
    }

    [IterationCleanup]
    public void CleanupIteration()
    {
        _catalogManager.Dispose();
        if (Directory.Exists(_tempDir))
        {
            try
            {
                Directory.Delete(_tempDir, recursive: true);
            }
            catch
            {
            }
        }
    }

    [Benchmark(Description = "CompactionPipeline.CompactAllAsync")]
    public async System.Threading.Tasks.Task<int> CompactAll()
    {
        var result = await _pipeline.CompactAllAsync();
        return result.TotalCompacted;
    }
}