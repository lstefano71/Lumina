using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Compaction;
using Lumina.Storage.Parquet;
using Lumina.Storage.Wal;

using Microsoft.Extensions.Logging.Abstractions;

using Xunit;

namespace Lumina.Tests.Storage;

/// <summary>
/// Integration tests for the compaction pipeline verifying WAL → Parquet conversion,
/// cursor tracking, and WAL file cleanup.
/// </summary>
public class CompactionIntegrationTests : WalTestBase
{
  private CompactionSettings CreateCompactionSettings() => new() {
    L1Directory = Path.Combine(TempDirectory, "l1"),
    CursorDirectory = Path.Combine(TempDirectory, "cursors"),
    MaxEntriesPerFile = 5,
    L1IntervalMinutes = 0
  };

  [Fact]
  public async Task CompactStream_ShouldCreateParquetFile()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    const string stream = "compact-create";
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    var entries = Enumerable.Range(0, 10).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = "info",
      Message = $"msg-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();
    await writer.WriteBatchAsync(entries);
    await walManager.ForceRotateAsync(stream);

    var compacted = await compactor.CompactStreamAsync(stream);

    compacted.Should().Be(10);
    var l1Files = compactor.GetL1Files(stream);
    l1Files.Should().HaveCount(1);
    File.Exists(l1Files[0]).Should().BeTrue();
  }

  [Fact]
  public async Task CompactStream_ParquetContent_ShouldMatchWal()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    const string stream = "compact-content";
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    var entries = Enumerable.Range(0, 15).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = i % 2 == 0 ? "info" : "warn",
      Message = $"content-{i}",
      Attributes = new Dictionary<string, object?> { ["seq"] = i }
    }).ToList();
    await writer.WriteBatchAsync(entries);
    await walManager.ForceRotateAsync(stream);

    await compactor.CompactStreamAsync(stream);

    var l1Files = compactor.GetL1Files(stream);
    var readEntries = new List<LogEntry>();
    foreach (var f in l1Files) {
      await foreach (var e in ParquetReader.ReadEntriesAsync(f)) {
        readEntries.Add(e);
      }
    }

    readEntries.Should().HaveCount(15);
    readEntries.Select(e => e.Message).Should().Contain("content-0");
    readEntries.Select(e => e.Message).Should().Contain("content-14");
  }

  [Fact]
  public async Task CompactStream_CursorAdvanced_ShouldNotRecompact()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    const string stream = "compact-idempotent";

    // Write and compact first batch
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    var batch = Enumerable.Range(0, 10).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = "info",
      Message = $"first-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();
    await writer.WriteBatchAsync(batch);
    await walManager.ForceRotateAsync(stream);

    var first = await compactor.CompactStreamAsync(stream);
    first.Should().Be(10);

    // Second call with no new data should compact 0
    var second = await compactor.CompactStreamAsync(stream);
    second.Should().Be(0);
  }

  [Fact]
  public async Task CompactStream_ShouldCleanSealedWalFiles()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    const string stream = "compact-cleanup";
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    var entries = Enumerable.Range(0, 10).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = "info",
      Message = $"cleanup-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();
    await writer.WriteBatchAsync(entries);
    await walManager.ForceRotateAsync(stream);

    var walFilesBefore = walManager.GetWalFiles(stream);
    walFilesBefore.Should().NotBeEmpty();

    await compactor.CompactStreamAsync(stream);

    // Sealed WAL files should be deleted; only the active writer file remains
    var walFilesAfter = walManager.GetWalFiles(stream);
    foreach (var f in walFilesAfter) {
      // The only remaining file should be the active writer's file
      var activeFile = walManager.GetActiveWriterFilePath(stream);
      f.Should().Be(activeFile);
    }
  }

  [Fact]
  public async Task CompactStream_WithAttributes_ShouldPreserveInParquet()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    const string stream = "compact-attrs";
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    var entries = Enumerable.Range(0, 20).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = "info",
      Message = $"attrs-{i}",
      Attributes = new Dictionary<string, object?> {
        ["host"] = "server-1",
        ["status_code"] = 200
      }
    }).ToList();
    await writer.WriteBatchAsync(entries);
    await walManager.ForceRotateAsync(stream);

    await compactor.CompactStreamAsync(stream);

    var l1Files = compactor.GetL1Files(stream);
    var readEntries = new List<LogEntry>();
    foreach (var f in l1Files) {
      await foreach (var e in ParquetReader.ReadEntriesAsync(f)) {
        readEntries.Add(e);
      }
    }

    readEntries.Should().HaveCount(20);
    readEntries.All(e => (string?)e.Attributes["host"] == "server-1").Should().BeTrue();
  }

  [Fact]
  public async Task CompactAll_NoStreams_ShouldReturnZero()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    var result = await compactor.CompactAllAsync();
    result.Should().Be(0);
  }
}
