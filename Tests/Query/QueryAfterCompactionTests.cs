using FluentAssertions;

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
/// Integration tests that verify DuckDB queries work correctly after
/// compaction changes the underlying Parquet files.
/// <para>
/// These tests exist because of a production bug where:
/// <list type="bullet">
///   <item><description>Views were created with <c>CREATE VIEW IF NOT EXISTS</c>
///   so refreshes were no-ops</description></item>
///   <item><description>Views were not refreshed after compaction, so they
///   referenced deleted L1 files</description></item>
/// </list>
/// </para>
/// </summary>
public sealed class QueryAfterCompactionTests : IDisposable
{
  private readonly string _testDir;
  private readonly string _l1Dir;
  private readonly string _l2Dir;
  private readonly string _catalogDir;
  private readonly CompactionSettings _compactionSettings;
  private readonly QuerySettings _querySettings;
  private readonly CatalogManager _catalogManager;

  public QueryAfterCompactionTests()
  {
    _testDir = Path.Combine(Path.GetTempPath(), $"query_compaction_test_{Guid.NewGuid():N}");
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
    if (Directory.Exists(_testDir)) {
      Directory.Delete(_testDir, recursive: true);
    }
  }

  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

  private async Task<string> WriteL1ParquetAsync(string stream, DateTime time, int count = 5)
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

    return path;
  }

  private ParquetManager CreateParquetManager()
  {
    return new ParquetManager(
        _compactionSettings,
        NullLogger<ParquetManager>.Instance,
        _catalogManager);
  }

  private CompactionPipeline CreatePipeline(params ICompactionTier[] tiers)
  {
    return new CompactionPipeline(
        _compactionSettings,
        _catalogManager,
        tiers,
        NullLogger<CompactionPipeline>.Instance);
  }

  private DuckDbQueryService CreateQueryService(ParquetManager parquetManager)
  {
    return new DuckDbQueryService(
        _querySettings,
        parquetManager,
        NullLogger<DuckDbQueryService>.Instance);
  }

  // -----------------------------------------------------------------------
  //  Tests
  // -----------------------------------------------------------------------

  [Fact]
  public async Task Query_AfterDailyCompaction_ReturnsCorrectData()
  {
    // Arrange: write L1 files for a closed day
    var stream = "compaction-query-daily";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    await WriteL1ParquetAsync(stream, yesterday, 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(1), 5);

    var parquetManager = CreateParquetManager();
    using var queryService = CreateQueryService(parquetManager);
    await queryService.InitializeAsync();

    // Sanity: query should work before compaction
    var beforeResult = await queryService.ExecuteQueryAsync(
        $"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)beforeResult.Rows[0]["cnt"]).Should().Be(10);

    // Act: run daily compaction — catalog updated, source files deferred for deletion
    var pipeline = CreatePipeline(new DailyCompactionTier());
    var compacted = await pipeline.CompactAllAsync();
    compacted.TotalCompacted.Should().BeGreaterThan(0);

    // Delete pending files and refresh views (simulating CompactorService)
    foreach (var files in compacted.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);
    await queryService.RefreshStreamsAsync();

    // Assert: query works and returns the same data from the L2 file
    var afterResult = await queryService.ExecuteQueryAsync(
        $"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)afterResult.Rows[0]["cnt"]).Should().Be(10);
  }

  [Fact]
  public async Task Query_AfterFullPipeline_ReturnsCorrectData()
  {
    // Arrange: write L1 files spanning a closed month
    var stream = "compaction-query-full";
    var jan10 = new DateTime(2024, 1, 10, 8, 0, 0, DateTimeKind.Utc);
    var jan20 = new DateTime(2024, 1, 20, 14, 0, 0, DateTimeKind.Utc);

    await WriteL1ParquetAsync(stream, jan10, 5);
    await WriteL1ParquetAsync(stream, jan20, 5);

    var parquetManager = CreateParquetManager();
    using var queryService = CreateQueryService(parquetManager);
    await queryService.InitializeAsync();

    // Sanity check
    var beforeResult = await queryService.ExecuteQueryAsync(
        $"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)beforeResult.Rows[0]["cnt"]).Should().Be(10);

    // Act: full pipeline (daily → monthly) — all original L1 files are gone
    var pipeline = CreatePipeline(new DailyCompactionTier(), new MonthlyCompactionTier());
    var compacted = await pipeline.CompactAllAsync();
    compacted.TotalCompacted.Should().BeGreaterThan(0);

    foreach (var files in compacted.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);
    await queryService.RefreshStreamsAsync();

    // Assert: query returns same data from the monthly L2 file
    var afterResult = await queryService.ExecuteQueryAsync(
        $"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)afterResult.Rows[0]["cnt"]).Should().Be(10);

    // Also verify content is correct, not just count
    var dataResult = await queryService.ExecuteQueryAsync(
        $"SELECT message FROM \"{stream}\" ORDER BY _t LIMIT 1");
    dataResult.Rows[0]["message"].Should().Be("msg-0");
  }

  [Fact]
  public async Task Query_WithoutRefresh_AfterCompaction_FailsOnDeletedFiles()
  {
    // This test documents the original bug: without view refresh,
    // queries fail because the view still references deleted L1 files.

    var stream = "stale-view-test";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    await WriteL1ParquetAsync(stream, yesterday, 5);

    var parquetManager = CreateParquetManager();
    using var queryService = CreateQueryService(parquetManager);
    await queryService.InitializeAsync();

    // Compaction updates catalog; files are deferred for deletion
    var pipeline = CreatePipeline(new DailyCompactionTier());
    var compactionResult = await pipeline.CompactAllAsync();

    // Manually delete pending files but do NOT refresh views — simulates the
    // original bug where stale views reference deleted source files
    foreach (var files in compactionResult.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);

    // Do NOT refresh views — the view still references the deleted L1 file

    // Assert: query should fail because the file no longer exists
    var act = () => queryService.ExecuteQueryAsync(
        $"SELECT count(*) AS cnt FROM \"{stream}\"");
    await act.Should().ThrowAsync<Exception>();
  }

  [Fact]
  public async Task ViewRefresh_ReplacesViewDefinition_NotSkippedAsNoop()
  {
    // Verifies that CREATE OR REPLACE VIEW actually updates the file
    // list, rather than being ignored by IF NOT EXISTS.

    var stream = "view-replace-test";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    // Start with one L1 file
    await WriteL1ParquetAsync(stream, yesterday, 3);

    var parquetManager = CreateParquetManager();
    using var queryService = CreateQueryService(parquetManager);
    await queryService.InitializeAsync();

    var result1 = await queryService.ExecuteQueryAsync(
        $"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)result1.Rows[0]["cnt"]).Should().Be(3);

    // Add more L1 files (simulating new ingestion)
    await WriteL1ParquetAsync(stream, yesterday.AddHours(2), 7);

    // Refresh — this must actually replace the view, not skip it
    await queryService.RefreshStreamsAsync();

    var result2 = await queryService.ExecuteQueryAsync(
        $"SELECT count(*) AS cnt FROM \"{stream}\"");
    ((long)result2.Rows[0]["cnt"]).Should().Be(10);
  }

  /// <summary>
  /// Regression: when compaction triggers RefreshStreamsAsync, the view must
  /// still include hot-buffer rows. A previous bug rebuilt the view from
  /// parquet files only, causing row counts to drop and then jump back up
  /// on the next LiveQueryRefreshService tick.
  /// </summary>
  [Fact]
  public async Task RefreshStreams_AfterCompaction_PreservesHotBufferRows()
  {
    var stream = "hot-plus-parquet";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    // Write parquet data and compact
    await WriteL1ParquetAsync(stream, yesterday, 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(1), 5);

    var parquetManager = CreateParquetManager();
    using var queryService = CreateQueryService(parquetManager);
    await queryService.InitializeAsync();

    // Add hot-buffer entries (simulating recent ingestion not yet compacted)
    var hotBuffer = new Lumina.Storage.Wal.WalHotBuffer();
    var hotEntries = Enumerable.Range(0, 8).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(-i),
      Level = "debug",
      Message = $"hot-{i}"
    }).ToList();

    foreach (var e in hotEntries)
      hotBuffer.Append(stream, "/wal/001.wal", hotEntries.IndexOf(e) * 50, e);

    var snapshot = hotBuffer.TakeSnapshot(stream);
    await queryService.RefreshHotBufferAsync(stream, snapshot);

    // Total should be parquet(10) + hot(8)
    var before = await queryService.ExecuteQueryAsync(
        $"SELECT COUNT(*) AS cnt FROM \"{stream}\"");
    var expectedTotal = (long)before.Rows[0]["cnt"];
    expectedTotal.Should().Be(18);

    // Simulate compaction: daily merge, delete old files, refresh views
    var pipeline = CreatePipeline(new DailyCompactionTier());
    var compacted = await pipeline.CompactAllAsync();
    foreach (var files in compacted.PendingDeletions.Values)
      pipeline.DeleteSourceFiles(files);

    await queryService.RefreshStreamsAsync();

    // Row count must be stable — compacted parquet(10) + hot(8)
    var after = await queryService.ExecuteQueryAsync(
        $"SELECT COUNT(*) AS cnt FROM \"{stream}\"");
    ((long)after.Rows[0]["cnt"]).Should().Be(expectedTotal,
        "RefreshStreamsAsync after compaction must not discard hot-buffer rows");
  }
}
