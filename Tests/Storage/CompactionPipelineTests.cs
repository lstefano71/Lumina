using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Catalog;
using Lumina.Storage.Compaction;
using Lumina.Storage.Parquet;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace Lumina.Tests.Storage;

/// <summary>
/// Tests for the N-tier calendar-based compaction pipeline (daily + monthly).
/// </summary>
public sealed class CompactionPipelineTests : IDisposable
{
  private readonly string _testDirectory;
  private readonly string _l1Directory;
  private readonly string _l2Directory;
  private readonly string _catalogDirectory;
  private readonly CompactionSettings _settings;
  private readonly CatalogManager _catalogManager;

  public CompactionPipelineTests()
  {
    _testDirectory = Path.Combine(Path.GetTempPath(), $"compaction_pipeline_test_{Guid.NewGuid():N}");
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

  /// <summary>Creates a pipeline with only the specified tiers.</summary>
  private CompactionPipeline CreatePipeline(params ICompactionTier[] tiers)
  {
    return new CompactionPipeline(
        _settings,
        _catalogManager,
        tiers,
        NullLogger<CompactionPipeline>.Instance);
  }

  /// <summary>Creates a pipeline with Daily + Monthly (default).</summary>
  private CompactionPipeline CreateDefaultPipeline()
      => CreatePipeline(new DailyCompactionTier(), new MonthlyCompactionTier());

  // -----------------------------------------------------------------------
  //  ICompactionTier unit tests — pure functions
  // -----------------------------------------------------------------------

  [Fact]
  public void DailyTier_Properties_AreCorrect()
  {
    var tier = new DailyCompactionTier();
    tier.Order.Should().Be(1);
    tier.Name.Should().Be("Daily");
    tier.InputLevel.Should().Be(StorageLevel.L1);
    tier.InputCompactionTier.Should().Be(1);
    tier.OutputCompactionTier.Should().Be(2);
    tier.MinGroupSize.Should().Be(1);
  }

  [Fact]
  public void MonthlyTier_Properties_AreCorrect()
  {
    var tier = new MonthlyCompactionTier();
    tier.Order.Should().Be(2);
    tier.Name.Should().Be("Monthly");
    tier.InputLevel.Should().Be(StorageLevel.L2);
    tier.InputCompactionTier.Should().Be(2);
    tier.OutputCompactionTier.Should().Be(3);
    tier.MinGroupSize.Should().Be(2);
  }

  [Fact]
  public void DailyTier_GroupEntries_GroupsByDay()
  {
    var tier = new DailyCompactionTier();
    var entries = new List<CatalogEntry> {
      new() { StreamName = "s", FilePath = "a", MinTime = new(2024, 1, 10, 8, 0, 0, DateTimeKind.Utc) },
      new() { StreamName = "s", FilePath = "b", MinTime = new(2024, 1, 10, 14, 0, 0, DateTimeKind.Utc) },
      new() { StreamName = "s", FilePath = "c", MinTime = new(2024, 1, 11, 2, 0, 0, DateTimeKind.Utc) },
    };

    var groups = tier.GroupEntries(entries).ToList();
    groups.Should().HaveCount(2);
    groups.Select(g => g.Key).Should().BeEquivalentTo(["20240110", "20240111"]);
    groups.First(g => g.Key == "20240110").Should().HaveCount(2);
  }

  [Fact]
  public void MonthlyTier_GroupEntries_GroupsByMonth()
  {
    var tier = new MonthlyCompactionTier();
    var entries = new List<CatalogEntry> {
      new() { StreamName = "s", FilePath = "a", MinTime = new(2024, 1, 5, 0, 0, 0, DateTimeKind.Utc) },
      new() { StreamName = "s", FilePath = "b", MinTime = new(2024, 1, 20, 0, 0, 0, DateTimeKind.Utc) },
      new() { StreamName = "s", FilePath = "c", MinTime = new(2024, 2, 1, 0, 0, 0, DateTimeKind.Utc) },
    };

    var groups = tier.GroupEntries(entries).ToList();
    groups.Should().HaveCount(2);
    groups.Select(g => g.Key).Should().BeEquivalentTo(["202401", "202402"]);
    groups.First(g => g.Key == "202401").Should().HaveCount(2);
  }

  [Fact]
  public void DailyTier_IsGroupClosed_ReturnsExpectedForPastAndFuture()
  {
    var tier = new DailyCompactionTier();
    var closedKey = "20240110"; // well in the past
    var openFutureKey = "29991231"; // always in the future

    tier.IsGroupClosed(closedKey).Should().BeTrue();
    tier.IsGroupClosed(openFutureKey).Should().BeFalse();
  }

  [Fact]
  public void MonthlyTier_IsGroupClosed_ReturnsExpectedForPastAndFuture()
  {
    var tier = new MonthlyCompactionTier();
    var closedKey = "202401"; // well in the past
    var openFutureKey = "299912"; // always in the future

    tier.IsGroupClosed(closedKey).Should().BeTrue();
    tier.IsGroupClosed(openFutureKey).Should().BeFalse();
  }

  [Fact]
  public void DailyTier_GetOutputFileName_MatchesConvention()
  {
    var tier = new DailyCompactionTier();
    tier.GetOutputFileName("my-stream", "20240110").Should().Be("my-stream_20240110.parquet");
  }

  [Fact]
  public void MonthlyTier_GetOutputFileName_MatchesConvention()
  {
    var tier = new MonthlyCompactionTier();
    tier.GetOutputFileName("my-stream", "202401").Should().Be("my-stream_202401.parquet");
  }

  // -----------------------------------------------------------------------
  //  Pipeline — Daily compaction
  // -----------------------------------------------------------------------

  [Fact]
  public async Task DailyCompaction_ShouldMergeL1FilesFromClosedDay()
  {
    var stream = "daily-test";
    var yesterday = DateTime.UtcNow.Date.AddDays(-1).AddHours(10);

    await WriteL1ParquetAsync(stream, yesterday, 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(1), 5);
    await WriteL1ParquetAsync(stream, yesterday.AddHours(2), 5);

    var pipeline = CreatePipeline(new DailyCompactionTier());
    var result = await pipeline.CompactAllAsync();

    result.TotalCompacted.Should().Be(3);
    // Perform deferred deletions (normally done by CompactorService under writer lock)
    foreach (var files in result.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);

    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(1);
    l2Entries[0].CompactionTier.Should().Be(2);
    l2Entries[0].RowCount.Should().Be(15);

    var fileName = Path.GetFileName(l2Entries[0].FilePath);
    fileName.Should().MatchRegex(@"^daily-test_\d{8}\.parquet$");

    var l1Entries = _catalogManager.GetEntries(stream, StorageLevel.L1);
    l1Entries.Should().BeEmpty();
  }

  [Fact]
  public async Task DailyCompaction_ShouldNotCompactOpenDay()
  {
    var stream = "open-day-test";
    var openDay = DateTime.UtcNow.Date.AddDays(1).AddHours(2);

    await WriteL1ParquetAsync(stream, openDay, 5);

    var pipeline = CreatePipeline(new DailyCompactionTier());
    var result = await pipeline.CompactAllAsync();

    result.TotalCompacted.Should().Be(0);

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

    var pipeline = CreatePipeline(new DailyCompactionTier());
    var result = await pipeline.CompactAllAsync();
    foreach (var files in result.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);

    result.TotalCompacted.Should().Be(2);

    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(2);
    l2Entries.Should().AllSatisfy(e => e.CompactionTier.Should().Be(2));
  }

  // -----------------------------------------------------------------------
  //  Pipeline — Monthly compaction
  // -----------------------------------------------------------------------

  [Fact]
  public async Task MonthlyCompaction_ShouldMergeDailyFilesFromClosedMonth()
  {
    var stream = "monthly-test";
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

    var pipeline = CreatePipeline(new MonthlyCompactionTier());
    var result = await pipeline.CompactAllAsync();
    foreach (var files in result.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);

    result.TotalCompacted.Should().Be(3);

    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(1);
    l2Entries[0].CompactionTier.Should().Be(3);
    l2Entries[0].RowCount.Should().Be(30);

    var fileName2 = Path.GetFileName(l2Entries[0].FilePath);
    fileName2.Should().Be("monthly-test_202401.parquet");
  }

  [Fact]
  public async Task MonthlyCompaction_ShouldNotCompactOpenMonth()
  {
    var stream = "open-month";
    var openMonth = new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, 1, 10, 0, 0, DateTimeKind.Utc).AddMonths(1);

    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    for (int day = 0; day < 2; day++) {
      var dayDate = openMonth.AddDays(day);
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

    var pipeline = CreatePipeline(new MonthlyCompactionTier());
    var result = await pipeline.CompactAllAsync();

    result.TotalCompacted.Should().Be(0);

    var l2Entries = _catalogManager.GetEntries(stream, StorageLevel.L2);
    l2Entries.Should().HaveCount(2);
    l2Entries.Should().AllSatisfy(e => e.CompactionTier.Should().Be(2));
  }

  [Fact]
  public async Task MonthlyCompaction_ShouldNotRecompactMonthlyFiles()
  {
    var stream = "no-recompact";
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

    var pipeline = CreatePipeline(new MonthlyCompactionTier());
    var result = await pipeline.CompactAllAsync();

    result.TotalCompacted.Should().Be(0);
  }

  // -----------------------------------------------------------------------
  //  Full pipeline (daily → monthly in one call)
  // -----------------------------------------------------------------------

  [Fact]
  public async Task CompactAll_ShouldRunDailyThenMonthly()
  {
    var stream = "full-pipeline";

    var jan1 = new DateTime(2024, 1, 10, 8, 0, 0, DateTimeKind.Utc);
    var jan2 = new DateTime(2024, 1, 20, 14, 0, 0, DateTimeKind.Utc);
    var jan3 = new DateTime(2024, 1, 25, 20, 0, 0, DateTimeKind.Utc);

    await WriteL1ParquetAsync(stream, jan1, 5);
    await WriteL1ParquetAsync(stream, jan2, 5);
    await WriteL1ParquetAsync(stream, jan3, 5);

    var pipeline = CreateDefaultPipeline();

    // Single CompactAllAsync chains daily → monthly in one pass:
    //   L1 files → daily L2 (3 files) → monthly L2 (1 file)
    var result = await pipeline.CompactAllAsync();
    result.TotalCompacted.Should().BeGreaterThan(0);
    foreach (var files in result.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);

    var allL2 = _catalogManager.GetEntries(stream, StorageLevel.L2);
    allL2.Should().HaveCount(1);
    allL2[0].CompactionTier.Should().Be(3);
    allL2[0].RowCount.Should().Be(15);

    var l1Entries = _catalogManager.GetEntries(stream, StorageLevel.L1);
    l1Entries.Should().BeEmpty();
  }

  [Fact]
  public async Task CompactAll_WithNoCatalog_ShouldReturnZero()
  {
    var pipeline = new CompactionPipeline(
        _settings,
        catalogManager: null,
        new ICompactionTier[] { new DailyCompactionTier(), new MonthlyCompactionTier() },
        NullLogger<CompactionPipeline>.Instance);

    var result = await pipeline.CompactAllAsync();
    result.TotalCompacted.Should().Be(0);
  }

  // -----------------------------------------------------------------------
  //  Tier ordering — ensures pipeline respects ICompactionTier.Order
  // -----------------------------------------------------------------------

  [Fact]
  public async Task Pipeline_ShouldRespectTierOrdering()
  {
    // Register monthly before daily — pipeline should still run daily first
    var pipeline = CreatePipeline(new MonthlyCompactionTier(), new DailyCompactionTier());

    var stream = "order-test";
    var jan1 = new DateTime(2024, 1, 10, 8, 0, 0, DateTimeKind.Utc);
    var jan2 = new DateTime(2024, 1, 20, 14, 0, 0, DateTimeKind.Utc);
    var jan3 = new DateTime(2024, 1, 25, 20, 0, 0, DateTimeKind.Utc);

    await WriteL1ParquetAsync(stream, jan1, 5);
    await WriteL1ParquetAsync(stream, jan2, 5);
    await WriteL1ParquetAsync(stream, jan3, 5);

    var result = await pipeline.CompactAllAsync();
    result.TotalCompacted.Should().BeGreaterThan(0);
    foreach (var files in result.PendingDeletions.Values) pipeline.DeleteSourceFiles(files);

    // Daily ran first → monthly consumed the daily files → one monthly file
    var allL2 = _catalogManager.GetEntries(stream, StorageLevel.L2);
    allL2.Should().HaveCount(1);
    allL2[0].CompactionTier.Should().Be(3);
  }

  // -----------------------------------------------------------------------
  //  CatalogManager.GetEligibleEntries — generic method
  // -----------------------------------------------------------------------

  [Fact]
  public async Task GetEligibleEntries_FiltersCorrectly()
  {
    var stream = "eligible-test";

    // L1 tier-1 file
    await WriteL1ParquetAsync(stream, DateTime.UtcNow.Date.AddDays(-1), 3);

    // L2 tier-2 file
    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);
    var dailyEntries = Enumerable.Range(0, 5).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc).AddHours(i),
      Level = "info",
      Message = $"msg-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();

    var dailyPath = Path.Combine(streamDir, $"{stream}_20240301.parquet");
    await ParquetWriter.WriteBatchAsync(dailyEntries, dailyPath, 100);
    await _catalogManager.AddFileAsync(new CatalogEntry {
      StreamName = stream,
      MinTime = dailyEntries.Min(e => e.Timestamp),
      MaxTime = dailyEntries.Max(e => e.Timestamp),
      FilePath = dailyPath,
      Level = StorageLevel.L2,
      RowCount = dailyEntries.Count,
      FileSizeBytes = new FileInfo(dailyPath).Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = 2
    });

    // Generic query for L1/tier-1
    var l1Entries = _catalogManager.GetEligibleEntries(stream, StorageLevel.L1, 1);
    l1Entries.Should().HaveCount(1);

    // Generic query for L2/tier-2
    var l2Entries = _catalogManager.GetEligibleEntries(stream, StorageLevel.L2, 2);
    l2Entries.Should().HaveCount(1);

    // Generic query for L2/tier-3 — should be empty
    var l3Entries = _catalogManager.GetEligibleEntries(stream, StorageLevel.L2, 3);
    l3Entries.Should().BeEmpty();
  }
}
