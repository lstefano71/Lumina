using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Query;
using Lumina.Storage.Catalog;
using Lumina.Storage.Compaction;
using Lumina.Storage.Parquet;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace Lumina.Tests.Storage;

/// <summary>
/// Tests for N-tier calendar-based L2 compaction (daily + monthly).
/// </summary>
public sealed class L2CompactorTests : IDisposable
{
  private readonly string _testDirectory;
  private readonly string _l1Directory;
  private readonly string _l2Directory;
  private readonly string _catalogDirectory;
  private readonly CompactionSettings _settings;
  private readonly CatalogManager _catalogManager;

  public L2CompactorTests()
  {
    _testDirectory = Path.Combine(Path.GetTempPath(), $"l2_compactor_test_{Guid.NewGuid():N}");
    _l1Directory = Path.Combine(_testDirectory, "l1");
    _l2Directory = Path.Combine(_testDirectory, "l2");
    _catalogDirectory = Path.Combine(_testDirectory, "catalog");

    Directory.CreateDirectory(_l1Directory);
    Directory.CreateDirectory(_l2Directory);
    Directory.CreateDirectory(_catalogDirectory);

    _settings = new CompactionSettings {
      L1Directory = _l1Directory,
      L2Directory = _l2Directory,
      CatalogDirectory = _catalogDirectory,
      MaxEntriesPerFile = 10000,
      L2IntervalHours = 0
    };

    var catalogOptions = new CatalogOptions {
      CatalogDirectory = _catalogDirectory,
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
    if (Directory.Exists(_testDirectory)) {
      Directory.Delete(_testDirectory, recursive: true);
    }
  }

  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

  private async Task<string> WriteL1ParquetAsync(string stream, DateTime time, int count = 5)
  {
    var streamDir = Path.Combine(_l1Directory, stream);
    Directory.CreateDirectory(streamDir);

    var entries = Enumerable.Range(0, count).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = time.AddSeconds(i),
      Level = "info",
      Message = $"msg-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();

    var fileName = $"{stream}_{time:yyyyMMdd_HHmmss}_{time.AddSeconds(count):yyyyMMdd_HHmmss}.parquet";
    var path = Path.Combine(streamDir, fileName);
    await ParquetWriter.WriteBatchAsync(entries, path, 100);

    var minTime = entries.Min(e => e.Timestamp);
    var maxTime = entries.Max(e => e.Timestamp);

    await _catalogManager.AddFileAsync(new CatalogEntry {
      StreamName = stream,
      MinTime = minTime,
      MaxTime = maxTime,
      FilePath = path,
      Level = StorageLevel.L1,
      RowCount = entries.Count,
      FileSizeBytes = new FileInfo(path).Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = 1
    });

    return path;
  }

  private L2Compactor CreateCompactor()
  {
    var parquetManager = new ParquetManager(
        _settings,
        NullLogger<ParquetManager>.Instance,
        _catalogManager);

    return new L2Compactor(
        _settings,
        parquetManager,
        NullLogger<L2Compactor>.Instance,
        _catalogManager);
  }

  // -----------------------------------------------------------------------
  //  Phase 1 — Daily compaction
  // -----------------------------------------------------------------------

  [Fact]
  public async Task DailyCompaction_ShouldMergeL1FilesFromClosedDay()
  {
    var stream = "daily-test";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    // Write 3 L1 files for yesterday
    await WriteL1ParquetAsync(stream, yesterday, 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(1), 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(2), 5);

    var compactor = CreateCompactor();
    var count = await compactor.CompactStreamDailyAsync(stream);

    count.Should().Be(3);

    // Verify daily L2 file was created
    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(1);
    l2Entries[0].CompactionTier.Should().Be(2);
    l2Entries[0].RowCount.Should().Be(15);

    // Verify filename convention: stream_yyyyMMdd.parquet
    var fileName = Path.GetFileName(l2Entries[0].FilePath);
    fileName.Should().MatchRegex(@"^daily-test_\d{8}\.parquet$");

    // L1 entries should be gone from catalog
    var l1Entries = _catalogManager.GetEntries(stream, StorageLevel.L1);
    l1Entries.Should().BeEmpty();
  }

  [Fact]
  public async Task DailyCompaction_ShouldNotCompactToday()
  {
    var stream = "today-test";
    var today = DateTime.UtcNow.Date.AddHours(2); // earlier today

    await WriteL1ParquetAsync(stream, today, 5);

    var compactor = CreateCompactor();
    var count = await compactor.CompactStreamDailyAsync(stream);

    count.Should().Be(0);

    // L1 entries should still be present
    var l1Entries = _catalogManager.GetEntries(stream, StorageLevel.L1);
    l1Entries.Should().HaveCount(1);
  }

  [Fact]
  public async Task DailyCompaction_ShouldHandleMultipleDays()
  {
    var stream = "multi-day";
    var twoDaysAgo = DateTime.UtcNow.Date.AddDays(-2).AddHours(14);
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(9);

    await WriteL1ParquetAsync(stream, twoDaysAgo, 3);
    await WriteL1ParquetAsync(stream, yesterday, 7);

    var compactor = CreateCompactor();
    var count = await compactor.CompactStreamDailyAsync(stream);

    count.Should().Be(2);

    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(2);
    l2Entries.Should().AllSatisfy(e => e.CompactionTier.Should().Be(2));
  }

  // -----------------------------------------------------------------------
  //  Phase 2 — Monthly compaction
  // -----------------------------------------------------------------------

  [Fact]
  public async Task MonthlyCompaction_ShouldMergeDailyFilesFromClosedMonth()
  {
    var stream = "monthly-test";

    // Create daily L2 files for a month that is definitely closed
    // Use January 2024 which is well in the past
    var baseDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    for (int day = 1; day <= 3; day++) {
      var dayDate = baseDate.AddDays(day - 1);

      var entries = Enumerable.Range(0, 10).Select(i => new LogEntry {
        Stream = stream,
        Timestamp = dayDate.AddHours(i),
        Level = "info",
        Message = $"day{day}-msg-{i}",
        Attributes = new Dictionary<string, object?>()
      }).ToList();

      var fileName = $"{stream}_{dayDate:yyyyMMdd}.parquet";
      var path = Path.Combine(streamDir, fileName);
      await ParquetWriter.WriteBatchAsync(entries, path, 100);

      await _catalogManager.AddFileAsync(new CatalogEntry {
        StreamName = stream,
        MinTime = entries.Min(e => e.Timestamp),
        MaxTime = entries.Max(e => e.Timestamp),
        FilePath = path,
        Level = StorageLevel.L2,
        RowCount = entries.Count,
        FileSizeBytes = new FileInfo(path).Length,
        AddedAt = DateTime.UtcNow,
        CompactionTier = 2
      });
    }

    var compactor = CreateCompactor();
    var count = await compactor.CompactStreamMonthlyAsync(stream);

    count.Should().Be(3);

    // Verify monthly L2 file was created
    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(1);
    l2Entries[0].CompactionTier.Should().Be(3);
    l2Entries[0].RowCount.Should().Be(30);

    // Verify filename convention: stream_yyyyMM.parquet
    var fileName2 = Path.GetFileName(l2Entries[0].FilePath);
    fileName2.Should().Be("monthly-test_202401.parquet");
  }

  [Fact]
  public async Task MonthlyCompaction_ShouldNotCompactCurrentMonth()
  {
    var stream = "current-month";
    var thisMonth = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, 1, 10, 0, 0, DateTimeKind.Utc);

    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    // create 2 daily L2 files for current month
    for (int day = 0; day < 2; day++) {
      var dayDate = thisMonth.AddDays(day);
      var entries = Enumerable.Range(0, 5).Select(i => new LogEntry {
        Stream = stream,
        Timestamp = dayDate.AddSeconds(i),
        Level = "info",
        Message = $"msg-{i}",
        Attributes = new Dictionary<string, object?>()
      }).ToList();

      var fileName = $"{stream}_{dayDate:yyyyMMdd}.parquet";
      var path = Path.Combine(streamDir, fileName);
      await ParquetWriter.WriteBatchAsync(entries, path, 100);

      await _catalogManager.AddFileAsync(new CatalogEntry {
        StreamName = stream,
        MinTime = entries.Min(e => e.Timestamp),
        MaxTime = entries.Max(e => e.Timestamp),
        FilePath = path,
        Level = StorageLevel.L2,
        RowCount = entries.Count,
        FileSizeBytes = new FileInfo(path).Length,
        AddedAt = DateTime.UtcNow,
        CompactionTier = 2
      });
    }

    var compactor = CreateCompactor();
    var count = await compactor.CompactStreamMonthlyAsync(stream);

    count.Should().Be(0);

    // Daily files should remain untouched
    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(2);
    l2Entries.Should().AllSatisfy(e => e.CompactionTier.Should().Be(2));
  }

  [Fact]
  public async Task MonthlyCompaction_ShouldNotRecompactMonthlyFiles()
  {
    var stream = "no-recompact";

    // Simulate an already-compacted monthly file (CompactionTier == 3)
    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    var entries = Enumerable.Range(0, 10).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddHours(i),
      Level = "info",
      Message = $"msg-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();

    var path = Path.Combine(streamDir, $"{stream}_202401.parquet");
    await ParquetWriter.WriteBatchAsync(entries, path, 100);

    await _catalogManager.AddFileAsync(new CatalogEntry {
      StreamName = stream,
      MinTime = entries.Min(e => e.Timestamp),
      MaxTime = entries.Max(e => e.Timestamp),
      FilePath = path,
      Level = StorageLevel.L2,
      RowCount = entries.Count,
      FileSizeBytes = new FileInfo(path).Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = 3
    });

    var compactor = CreateCompactor();
    var count = await compactor.CompactStreamMonthlyAsync(stream);

    count.Should().Be(0);
  }

  // -----------------------------------------------------------------------
  //  Full pipeline (daily → monthly in one call)
  // -----------------------------------------------------------------------

  [Fact]
  public async Task CompactAll_ShouldRunDailyThenMonthly()
  {
    var stream = "full-pipeline";

    // Write L1 files for multiple days across a closed month (e.g., Jan 2024)
    var jan1 = new DateTime(2024, 1, 10, 8, 0, 0, DateTimeKind.Utc);
    var jan2 = new DateTime(2024, 1, 20, 14, 0, 0, DateTimeKind.Utc);
    var jan3 = new DateTime(2024, 1, 25, 20, 0, 0, DateTimeKind.Utc);

    await WriteL1ParquetAsync(stream, jan1, 5);
    await WriteL1ParquetAsync(stream, jan2, 5);
    await WriteL1ParquetAsync(stream, jan3, 5);

    var compactor = CreateCompactor();

    // Single CompactAllAsync call chains daily → monthly in one pass:
    //   L1 files → daily L2 (3 files) → monthly L2 (1 file)
    var result = await compactor.CompactAllAsync();
    result.Should().BeGreaterThan(0);

    // After the full pipeline: should have one monthly file (CompactionTier == 3)
    var allL2 = _catalogManager.GetEntries(stream, StorageLevel.L2);
    allL2.Should().HaveCount(1);
    allL2[0].CompactionTier.Should().Be(3);
    allL2[0].RowCount.Should().Be(15);

    // L1 entries should be gone
    var l1Entries = _catalogManager.GetEntries(stream, StorageLevel.L1);
    l1Entries.Should().BeEmpty();
  }

  [Fact]
  public async Task CompactAll_WithNoCatalog_ShouldReturnZero()
  {
    var parquetManager = new ParquetManager(
        _settings,
        NullLogger<ParquetManager>.Instance,
        _catalogManager);

    var compactor = new L2Compactor(
        _settings,
        parquetManager,
        NullLogger<L2Compactor>.Instance,
        catalogManager: null);

    var count = await compactor.CompactAllAsync();
    count.Should().Be(0);
  }
}
