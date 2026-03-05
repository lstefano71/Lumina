using FluentAssertions;

using Lumina.Core.Concurrency;
using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Query;
using Lumina.Storage.Catalog;
using Lumina.Storage.Compaction;
using Lumina.Storage.Parquet;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace Lumina.Tests.Query;

/// <summary>
/// Integration tests verifying that the <see cref="AsyncReaderWriterLock"/>-based
/// concurrency control prevents race conditions between in-flight DuckDB queries
/// and compaction file-swap + delete operations.
/// </summary>
public sealed class QueryVsCompactionTests : IDisposable
{
  private readonly string _testDir;
  private readonly string _l1Dir;
  private readonly string _l2Dir;
  private readonly string _catalogDir;
  private readonly CompactionSettings _compactionSettings;
  private readonly QuerySettings _querySettings;
  private readonly CatalogManager _catalogManager;
  private readonly StreamLockManager _streamLockManager;

  public QueryVsCompactionTests()
  {
    _testDir = Path.Combine(Path.GetTempPath(), $"query_vs_compact_{Guid.NewGuid():N}");
    _l1Dir = Path.Combine(_testDir, "l1");
    _l2Dir = Path.Combine(_testDir, "l2");
    _catalogDir = Path.Combine(_testDir, "catalog");

    Directory.CreateDirectory(_l1Dir);
    Directory.CreateDirectory(_l2Dir);
    Directory.CreateDirectory(_catalogDir);

    _compactionSettings = new CompactionSettings {
      L1Directory = _l1Dir,
      L2Directory = _l2Dir,
      CatalogDirectory = _catalogDir,
      MaxEntriesPerFile = 10000,
      L2IntervalHours = 0
    };

    _querySettings = new QuerySettings();
    _streamLockManager = new StreamLockManager();

    var catalogOptions = new CatalogOptions {
      CatalogDirectory = _catalogDir,
      EnableAutoRebuild = false,
      EnableStartupGc = false
    };

    _catalogManager = new CatalogManager(
        catalogOptions,
        NullLogger<CatalogManager>.Instance);

    _catalogManager.InitializeAsync().GetAwaiter().GetResult();
  }

  public void Dispose()
  {
    _catalogManager.Dispose();
    if (Directory.Exists(_testDir))
      Directory.Delete(_testDir, recursive: true);
  }

  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

  private async Task WriteL1ParquetAsync(string stream, DateTime time, int count)
  {
    var streamDir = Path.Combine(_l1Dir, stream);
    Directory.CreateDirectory(streamDir);

    var entries = Enumerable.Range(0, count).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = time.AddSeconds(i),
      Level = "info",
      Message = $"msg-{i}",
      Attributes = new Dictionary<string, object?> { ["index"] = i }
    }).ToList();

    var fileName = $"{stream}_{time:yyyyMMdd_HHmmss}_{time.AddSeconds(count):yyyyMMdd_HHmmss}.parquet";
    var path = Path.Combine(streamDir, fileName);
    await ParquetWriter.WriteBatchAsync(entries, path, 100);

    await _catalogManager.AddFileAsync(new CatalogEntry {
      StreamName = stream,
      MinTime = entries.Min(e => e.Timestamp),
      MaxTime = entries.Max(e => e.Timestamp),
      FilePath = path,
      Level = StorageLevel.L1,
      RowCount = entries.Count,
      FileSizeBytes = new FileInfo(path).Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = 1
    });
  }

  private ParquetManager CreateParquetManager()
    => new(_compactionSettings, NullLogger<ParquetManager>.Instance, _catalogManager);

  private DuckDbQueryService CreateQueryService(ParquetManager pm)
    => new(_querySettings, pm, NullLogger<DuckDbQueryService>.Instance, _streamLockManager);

  private CompactionPipeline CreatePipeline(params ICompactionTier[] tiers)
    => new(_compactionSettings, _catalogManager, tiers,
        NullLogger<CompactionPipeline>.Instance, _streamLockManager);

  // -----------------------------------------------------------------------
  //  Tests
  // -----------------------------------------------------------------------

  /// <summary>
  /// Verifies that a query holding a reader lock prevents the compaction
  /// writer from deleting files until the query is complete.
  /// </summary>
  [Fact]
  public async Task ReaderLock_PreventsWriterDeletion_WhileQueryActive()
  {
    var stream = "race-test";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    await WriteL1ParquetAsync(stream, yesterday, 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(1), 5);

    var pm = CreateParquetManager();
    using var qs = CreateQueryService(pm);
    await qs.InitializeAsync();

    // Acquire a reader lock (simulating a long-running query)
    await using var readerGuard = await _streamLockManager.CompactionLock
        .ReaderLockAsync();

    // Start a writer lock attempt in the background — it should block
    var writerAcquired = false;
    var writerTask = Task.Run(async () => {
      await using var writerGuard = await _streamLockManager.CompactionLock
          .WriterLockAsync();
      writerAcquired = true;
    });

    // Give the writer a chance to try
    await Task.Delay(200);

    // Writer must NOT have acquired the lock yet
    writerAcquired.Should().BeFalse("writer should be blocked while reader holds the lock");

    // Release the reader guard — this should unblock the writer
    await readerGuard.DisposeAsync();

    // Writer should complete now
    await writerTask.WaitAsync(TimeSpan.FromSeconds(5));
    writerAcquired.Should().BeTrue();
  }

  /// <summary>
  /// Verifies that multiple concurrent queries (readers) can execute
  /// simultaneously without blocking each other.
  /// </summary>
  [Fact]
  public async Task MultipleReaders_DoNotBlockEachOther()
  {
    var stream = "concurrent-reads";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    await WriteL1ParquetAsync(stream, yesterday, 10);

    var pm = CreateParquetManager();
    using var qs = CreateQueryService(pm);
    await qs.InitializeAsync();

    // Launch several concurrent queries
    var tasks = Enumerable.Range(0, 5).Select(_ =>
        qs.ExecuteQueryAsync($"SELECT count(*) AS cnt FROM \"{stream}\""))
      .ToList();

    var results = await Task.WhenAll(tasks);

    results.Should().AllSatisfy(r => {
      r.Rows.Should().HaveCount(1);
      ((long)r.Rows[0]["cnt"]!).Should().Be(10);
    });
  }

  /// <summary>
  /// End-to-end: runs compaction under writer lock while a query is
  /// blocked, then verifies the query succeeds after the writer releases.
  /// Simulates the full CompactorService flow.
  /// </summary>
  [Fact]
  public async Task CompactionUnderWriterLock_ThenQuery_Succeeds()
  {
    var stream = "e2e-lock-test";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    await WriteL1ParquetAsync(stream, yesterday, 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(1), 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(2), 5);

    var pm = CreateParquetManager();
    using var qs = CreateQueryService(pm);
    await qs.InitializeAsync();

    // Validate baseline
    var before = await qs.ExecuteQueryAsync($"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)before.Rows[0]["cnt"]!).Should().Be(15);

    // Run compaction
    var pipeline = CreatePipeline(new DailyCompactionTier());
    var result = await pipeline.CompactAllAsync();
    result.TotalCompacted.Should().BeGreaterThan(0);

    // Simulate CompactorService: acquire writer lock → refresh views → delete files
    await using (var _ = await _streamLockManager.CompactionLock.WriterLockAsync()) {
      await qs.RefreshStreamsAsync();
      foreach (var files in result.PendingDeletions.Values)
        pipeline.DeleteSourceFiles(files);
    }

    // Query must succeed against the compacted L2 file
    var after = await qs.ExecuteQueryAsync($"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)after.Rows[0]["cnt"]!).Should().Be(15);
  }

  /// <summary>
  /// Verifies that the <see cref="AsyncReaderWriterLock"/> correctly
  /// serialises writers: only one writer can hold the lock.
  /// </summary>
  [Fact]
  public async Task WriterLock_ExcludesOtherWriters()
  {
    await using var writerGuard = await _streamLockManager.CompactionLock
        .WriterLockAsync();

    var secondWriterAcquired = false;
    var secondWriterTask = Task.Run(async () => {
      await using var w2 = await _streamLockManager.CompactionLock.WriterLockAsync();
      secondWriterAcquired = true;
    });

    await Task.Delay(200);
    secondWriterAcquired.Should().BeFalse("second writer should block while first writer holds the lock");

    // Release first writer — second should acquire
    await writerGuard.DisposeAsync();

    await secondWriterTask.WaitAsync(TimeSpan.FromSeconds(5));
    secondWriterAcquired.Should().BeTrue();
  }
}
