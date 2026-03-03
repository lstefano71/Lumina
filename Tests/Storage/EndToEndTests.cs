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
/// End-to-end integration tests that exercise the full pipeline:
/// Ingest → WAL → Compaction → Parquet → Read.
/// </summary>
public class EndToEndTests : WalTestBase
{
  [Fact]
  public async Task FullPipeline_IngestCompactRead_ShouldRoundTrip()
  {
    // Arrange
    var walSettings = GetTestSettings();
    var compactionSettings = new CompactionSettings {
      L1Directory = Path.Combine(TempDirectory, "l1"),
      CursorDirectory = Path.Combine(TempDirectory, "cursors"),
      MaxEntriesPerFile = 10, // low threshold to trigger compaction
      L1IntervalMinutes = 0   // immediate
    };

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var logger = NullLogger<L1Compactor>.Instance;
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings, logger);

    const string stream = "e2e-stream";
    var entries = Enumerable.Range(0, 25).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = "info",
      Message = $"End-to-end message {i}",
      Attributes = new Dictionary<string, object?> { ["index"] = i }
    }).ToList();

    // Act – Ingest into WAL
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    await writer.WriteBatchAsync(entries);

    // Force-rotate so compactor sees sealed files
    await walManager.ForceRotateAsync(stream);

    // Compact WAL → Parquet
    var compacted = await compactor.CompactAllAsync();

    // Assert – compaction ran
    compacted.Should().Be(25);

    // Read back from Parquet
    var l1Files = compactor.GetL1Files(stream);
    l1Files.Should().NotBeEmpty();

    var readEntries = new List<LogEntry>();
    foreach (var file in l1Files) {
      await foreach (var entry in ParquetReader.ReadEntriesAsync(file)) {
        readEntries.Add(entry);
      }
    }

    readEntries.Should().HaveCount(25);
    readEntries.Select(e => e.Message).Should().Contain("End-to-end message 0");
    readEntries.Select(e => e.Message).Should().Contain("End-to-end message 24");
  }

  [Fact]
  public async Task FullPipeline_MultipleStreams_ShouldCompactIndependently()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = new CompactionSettings {
      L1Directory = Path.Combine(TempDirectory, "l1"),
      CursorDirectory = Path.Combine(TempDirectory, "cursors"),
      MaxEntriesPerFile = 5,
      L1IntervalMinutes = 0
    };

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    // Ingest into two separate streams
    foreach (var stream in new[] { "stream-a", "stream-b" }) {
      var writer = await walManager.GetOrCreateWriterAsync(stream);
      var entries = Enumerable.Range(0, 10).Select(i => new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow.AddSeconds(i),
        Level = "info",
        Message = $"{stream} msg {i}",
        Attributes = new Dictionary<string, object?>()
      }).ToList();
      await writer.WriteBatchAsync(entries);
      await walManager.ForceRotateAsync(stream);
    }

    var compacted = await compactor.CompactAllAsync();
    compacted.Should().Be(20);

    compactor.GetL1Files("stream-a").Should().NotBeEmpty();
    compactor.GetL1Files("stream-b").Should().NotBeEmpty();
  }

  [Fact]
  public async Task FullPipeline_CompactionCursor_ShouldAdvance()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = new CompactionSettings {
      L1Directory = Path.Combine(TempDirectory, "l1"),
      CursorDirectory = Path.Combine(TempDirectory, "cursors"),
      MaxEntriesPerFile = 5,
      L1IntervalMinutes = 0
    };

    await using var walManager = new WalManager(walSettings);
    var cursorManager = new CursorManager(compactionSettings.CursorDirectory);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    const string stream = "cursor-test";

    // First batch
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    var batch1 = Enumerable.Range(0, 10).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(i),
      Level = "info",
      Message = $"batch1-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();
    await writer.WriteBatchAsync(batch1);
    await walManager.ForceRotateAsync(stream);

    await compactor.CompactAllAsync();
    var cursor1 = cursorManager.GetCursor(stream);
    cursor1.LastCompactedOffset.Should().BeGreaterThan(0);

    // Second batch
    writer = await walManager.GetOrCreateWriterAsync(stream);
    var batch2 = Enumerable.Range(0, 10).Select(i => new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(100 + i),
      Level = "warn",
      Message = $"batch2-{i}",
      Attributes = new Dictionary<string, object?>()
    }).ToList();
    await writer.WriteBatchAsync(batch2);
    await walManager.ForceRotateAsync(stream);

    await compactor.CompactAllAsync();
    var cursor2 = cursorManager.GetCursor(stream);

    // Cursor should have advanced
    (cursor2.LastCompactedOffset > cursor1.LastCompactedOffset ||
     string.Compare(cursor2.LastCompactedWalFile, cursor1.LastCompactedWalFile, StringComparison.Ordinal) > 0)
        .Should().BeTrue("cursor should advance after second compaction");
  }
}
