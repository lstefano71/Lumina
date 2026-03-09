using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Catalog;
using Lumina.Storage.Parquet;

using Microsoft.Extensions.Logging;

using Xunit;

namespace Lumina.Tests.Storage.Catalog;

public sealed class CatalogRebuilderTests : IDisposable
{
  private readonly string _testDirectory;
  private readonly string _l1Directory;
  private readonly string _l2Directory;
  private readonly ILogger<CatalogRebuilder> _logger;
  private readonly CatalogRebuilder _rebuilder;

  public CatalogRebuilderTests()
  {
    _testDirectory = Path.Combine(Path.GetTempPath(), $"catalog_rebuilder_test_{Guid.NewGuid():N}");
    _l1Directory = Path.Combine(_testDirectory, "L1");
    _l2Directory = Path.Combine(_testDirectory, "L2");

    Directory.CreateDirectory(_l1Directory);
    Directory.CreateDirectory(_l2Directory);

    _logger = LoggerFactory.Create(builder => builder.AddDebug()).CreateLogger<CatalogRebuilder>();
    _rebuilder = new CatalogRebuilder(_logger);
  }

  public void Dispose()
  {
    if (Directory.Exists(_testDirectory)) {
      Directory.Delete(_testDirectory, recursive: true);
    }
  }

  [Fact]
  public async Task RecoverFromDiskAsync_ShouldReturnEmptyCatalog_WhenNoFiles()
  {
    var catalog = await _rebuilder.RecoverFromDiskAsync(_l1Directory, _l2Directory);

    Assert.Empty(catalog.Entries);
  }

  [Fact]
  public async Task ResolveConflicts_WhenL2OverlapsL1_ShouldPreferL2()
  {
    var baseTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    // L2 file covering 10:00 - 14:00
    var l2Entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = baseTime.AddHours(-2),
      MaxTime = baseTime.AddHours(2),
      FilePath = Path.Combine(_l2Directory, "test-stream_20240101.parquet"),
      Level = StorageLevel.L2,
      RowCount = 100,
      FileSizeBytes = 768,
      AddedAt = DateTime.UtcNow
    };

    // L1 file overlapping with L2 (11:00 - 13:00)
    var l1Entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = baseTime.AddHours(-1),
      MaxTime = baseTime.AddHours(1),
      FilePath = Path.Combine(_l1Directory, "test-stream_20240101_120000.parquet"),
      Level = StorageLevel.L1,
      RowCount = 100,
      FileSizeBytes = 1024,
      AddedAt = DateTime.UtcNow
    };

    var entries = new List<CatalogEntry> { l1Entry, l2Entry };

    var resolved = _rebuilder.ResolveConflicts(entries);

    Assert.Single(resolved.Entries);
    Assert.Equal(StorageLevel.L2, resolved.Entries[0].Level);
  }

  [Fact]
  public async Task ResolveConflicts_WhenNoOverlap_ShouldIncludeBoth()
  {
    var baseTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    // L2 file covering 08:00 - 10:00
    var l2Entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = baseTime.AddHours(-4),
      MaxTime = baseTime.AddHours(-2),
      FilePath = Path.Combine(_l2Directory, "test-stream_20240101_morning.parquet"),
      Level = StorageLevel.L2,
      RowCount = 100,
      FileSizeBytes = 768,
      AddedAt = DateTime.UtcNow
    };

    // L1 file not overlapping (14:00 - 16:00)
    var l1Entry = new CatalogEntry {
      StreamName = "test-stream",
      MinTime = baseTime.AddHours(2),
      MaxTime = baseTime.AddHours(4),
      FilePath = Path.Combine(_l1Directory, "test-stream_20240101_afternoon.parquet"),
      Level = StorageLevel.L1,
      RowCount = 50,
      FileSizeBytes = 512,
      AddedAt = DateTime.UtcNow
    };

    var entries = new List<CatalogEntry> { l1Entry, l2Entry };

    var resolved = _rebuilder.ResolveConflicts(entries);

    Assert.Equal(2, resolved.Entries.Count);
  }

  [Fact]
  public async Task ResolveConflicts_WhenNoL2Exists_ShouldIncludeL1Files()
  {
    var baseTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    var entries = new List<CatalogEntry>
    {
      new()
      {
        StreamName = "test-stream",
        MinTime = baseTime.AddHours(-1),
        MaxTime = baseTime,
        FilePath = Path.Combine(_l1Directory, "test-stream_20240101_120000.parquet"),
        Level = StorageLevel.L1,
        RowCount = 100,
        FileSizeBytes = 1024,
        AddedAt = DateTime.UtcNow
      },
      new()
      {
        StreamName = "test-stream",
        MinTime = baseTime,
        MaxTime = baseTime.AddHours(1),
        FilePath = Path.Combine(_l1Directory, "test-stream_20240101_130000.parquet"),
        Level = StorageLevel.L1,
        RowCount = 50,
        FileSizeBytes = 512,
        AddedAt = DateTime.UtcNow
      }
    };

    var resolved = _rebuilder.ResolveConflicts(entries);

    Assert.Equal(2, resolved.Entries.Count);
    Assert.All(resolved.Entries, e => Assert.Equal(StorageLevel.L1, e.Level));
  }

  [Fact]
  public async Task ResolveConflicts_ShouldGroupByStream()
  {
    var baseTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    // stream-a L2 file at 08:00 - 10:00 (non-overlapping with L1)
    // stream-a L1 file at 14:00 - 16:00 (non-overlapping with L2)
    // stream-b L1 file at 11:00 - 12:00 (different stream)
    var entries = new List<CatalogEntry>
    {
      new()
      {
        StreamName = "stream-a",
        MinTime = baseTime.AddHours(-4),
        MaxTime = baseTime.AddHours(-2),
        FilePath = "a_20240101_l2.parquet",
        Level = StorageLevel.L2,
        RowCount = 200,
        FileSizeBytes = 2048,
        AddedAt = DateTime.UtcNow
      },
      new()
      {
        StreamName = "stream-a",
        MinTime = baseTime.AddHours(2),
        MaxTime = baseTime.AddHours(4),
        FilePath = "a_20240102_l1.parquet",
        Level = StorageLevel.L1,
        RowCount = 100,
        FileSizeBytes = 1024,
        AddedAt = DateTime.UtcNow
      },
      new()
      {
        StreamName = "stream-b",
        MinTime = baseTime.AddHours(-1),
        MaxTime = baseTime,
        FilePath = "b_20240101.parquet",
        Level = StorageLevel.L1,
        RowCount = 50,
        FileSizeBytes = 512,
        AddedAt = DateTime.UtcNow
      }
    };

    var resolved = _rebuilder.ResolveConflicts(entries);

    Assert.Equal(3, resolved.Entries.Count);
  }

  [Fact]
  public async Task ScanL1FilesAsync_ShouldReturnEmpty_WhenDirectoryEmpty()
  {
    var entries = await _rebuilder.ScanL1FilesAsync(_l1Directory);

    Assert.Empty(entries);
  }

  [Fact]
  public async Task ScanL2FilesAsync_ShouldReturnEmpty_WhenDirectoryEmpty()
  {
    var entries = await _rebuilder.ScanL2FilesAsync(_l2Directory);

    Assert.Empty(entries);
  }

  [Fact]
  public async Task ScanL1FilesAsync_ShouldReturnEmpty_WhenDirectoryNotExists()
  {
    var entries = await _rebuilder.ScanL1FilesAsync("/nonexistent/path");

    Assert.Empty(entries);
  }

  // -----------------------------------------------------------------------
  //  CompactionTier inference from embedded Parquet file metadata
  // -----------------------------------------------------------------------

  [Fact]
  public async Task RecoverFromDisk_DailyL2File_WithCompactionTierMetadata_ShouldRebuildAsTier2()
  {
    var stream = "rebuild-daily";
    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    var path = Path.Combine(streamDir, $"{stream}_20240110.parquet");
    await WriteL2ParquetAsync(path, stream,
        new DateTime(2024, 1, 10, 0, 0, 0, DateTimeKind.Utc), 5, compactionTier: 2);

    var catalog = await _rebuilder.RecoverFromDiskAsync(_l1Directory, _l2Directory);

    catalog.Entries.Should().ContainSingle();
    catalog.Entries[0].CompactionTier.Should().Be(2);
  }

  [Fact]
  public async Task RecoverFromDisk_MonthlyL2File_WithCompactionTierMetadata_ShouldRebuildAsTier3()
  {
    var stream = "rebuild-monthly";
    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    var path = Path.Combine(streamDir, $"{stream}_202401.parquet");
    await WriteL2ParquetAsync(path, stream,
        new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), 10, compactionTier: 3);

    var catalog = await _rebuilder.RecoverFromDiskAsync(_l1Directory, _l2Directory);

    catalog.Entries.Should().ContainSingle();
    catalog.Entries[0].CompactionTier.Should().Be(3,
        because: "monthly output files carry tier 3 in Parquet metadata");
  }

  [Fact]
  public async Task RecoverFromDisk_L2FileWithoutMetadata_ShouldFallbackToIntLevel()
  {
    var stream = "rebuild-legacy";
    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    // WriteBatchAsync does not embed metadata — simulates pre-metadata files
    var path = Path.Combine(streamDir, $"{stream}_20240110.parquet");
    var entries = BuildEntries(stream, new DateTime(2024, 1, 10, 0, 0, 0, DateTimeKind.Utc), 5);
    await ParquetWriter.WriteBatchAsync(entries, path);

    var catalog = await _rebuilder.RecoverFromDiskAsync(_l1Directory, _l2Directory);

    catalog.Entries.Should().ContainSingle();
    catalog.Entries[0].CompactionTier.Should().Be((int)StorageLevel.L2,
        because: "files without metadata fall back to (int)StorageLevel");
  }

  [Fact]
  public async Task RecoverFromDisk_MixedTierL2Files_ShouldInferCorrectTierPerFile()
  {
    var stream = "rebuild-mixed";
    var streamDir = Path.Combine(_l2Directory, stream);
    Directory.CreateDirectory(streamDir);

    // A daily file from January 10 (tier 2)
    var dailyPath = Path.Combine(streamDir, $"{stream}_20240110.parquet");
    await WriteL2ParquetAsync(dailyPath, stream,
        new DateTime(2024, 1, 10, 0, 0, 0, DateTimeKind.Utc), 5, compactionTier: 2);

    // A monthly file from February (tier 3) — non-overlapping time range
    var monthlyPath = Path.Combine(streamDir, $"{stream}_202402.parquet");
    await WriteL2ParquetAsync(monthlyPath, stream,
        new DateTime(2024, 2, 1, 0, 0, 0, DateTimeKind.Utc), 5, compactionTier: 3);

    var catalog = await _rebuilder.RecoverFromDiskAsync(_l1Directory, _l2Directory);

    catalog.Entries.Should().HaveCount(2);
    catalog.Entries.Should().ContainSingle(e => e.CompactionTier == 2);
    catalog.Entries.Should().ContainSingle(e => e.CompactionTier == 3);
  }

  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

  private static List<LogEntry> BuildEntries(string stream, DateTime baseTime, int count) =>
      Enumerable.Range(0, count)
          .Select(i => new LogEntry {
            Stream = stream,
            Timestamp = baseTime.AddSeconds(i),
            Level = "info",
            Message = $"msg-{i}",
            Attributes = new Dictionary<string, object?>()
          })
          .ToList();

  private static async Task WriteL2ParquetAsync(
      string path, string stream, DateTime baseTime, int count, int compactionTier)
  {
    var entries = BuildEntries(stream, baseTime, count);

    async IAsyncEnumerable<IReadOnlyList<LogEntry>> SingleChunk(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
      yield return entries;
    }

    await ParquetWriter.WriteChunkedAsync(
        SingleChunk(),
        path,
        fileMetadata: new Dictionary<string, string> {
          ["lumina.compaction_tier"] = compactionTier.ToString()
        });
  }
}