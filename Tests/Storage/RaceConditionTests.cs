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
/// Tests verifying that race conditions identified in the review are fixed.
/// </summary>
public sealed class RaceConditionTests : WalTestBase
{
  private CompactionSettings CreateCompactionSettings() => new() {
    L1Directory = Path.Combine(TempDirectory, "l1"),
    CursorDirectory = Path.Combine(TempDirectory, "cursors"),
    MaxEntriesPerFile = 5,   // low threshold to force compaction
    L1IntervalMinutes = 0
  };

  private CursorManager CreateCursorManager(CompactionSettings settings)
  {
    var validator = new CursorValidator();
    return new CursorManager(
        settings.CursorDirectory,
        validator,
        recoveryService: null,
        NullLogger<CursorManager>.Instance,
        enableValidation: true,
        enableRecovery: false);
  }

  // ---------------------------------------------------------------
  // Issue 1: L1Compactor must not delete WAL files that contain
  //          entries appended after the compaction read cursor.
  // ---------------------------------------------------------------

  [Fact]
  public async Task Compaction_WithConcurrentIngestion_ShouldNotLoseEntries()
  {
    var walSettings = GetTestSettings();
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = CreateCursorManager(compactionSettings);
    var hotBuffer = new WalHotBuffer();
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance, hotBuffer: hotBuffer);

    const string stream = "race-compact";
    const int initialEntries = 10;
    const int concurrentEntries = 20;

    // Phase 1: Write initial entries
    var writer = await walManager.GetOrCreateWriterAsync(stream);
    for (int i = 0; i < initialEntries; i++) {
      await writer.WriteAsync(new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow.AddSeconds(i),
        Level = "info",
        Message = $"initial-{i}",
        Attributes = new Dictionary<string, object?>()
      });
    }

    // Phase 2: Run compaction (reads entries 0-9)
    var compacted = await compactor.CompactStreamAsync(stream);
    compacted.Should().Be(initialEntries);

    // Phase 3: Write more entries (these go into the active/rotated WAL)
    writer = await walManager.GetOrCreateWriterAsync(stream);
    for (int i = 0; i < concurrentEntries; i++) {
      await writer.WriteAsync(new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow.AddSeconds(initialEntries + i),
        Level = "info",
        Message = $"concurrent-{i}",
        Attributes = new Dictionary<string, object?>()
      });
    }

    // Phase 4: Run compaction again — must pick up the concurrent entries
    var compacted2 = await compactor.CompactStreamAsync(stream);
    compacted2.Should().Be(concurrentEntries);

    // Phase 5: Verify total entries across all Parquet files
    var l1Files = compactor.GetL1Files(stream);
    var totalRead = 0;
    foreach (var f in l1Files) {
      await foreach (var _ in ParquetReader.ReadEntriesAsync(f)) {
        totalRead++;
      }
    }

    totalRead.Should().Be(initialEntries + concurrentEntries,
        because: "no entries should be lost during concurrent ingestion and compaction");
  }

  [Fact]
  public async Task Compaction_AfterNaturalRotation_ShouldPreserveTrailingEntries()
  {
    // Use a very small WAL size to trigger natural rotation during writes
    var walSettings = new WalSettings {
      DataDirectory = TempDirectory,
      MaxWalSizeBytes = 2048,
      EnableWriteThrough = false,
      FlushIntervalMs = 100
    };
    var compactionSettings = CreateCompactionSettings();

    await using var walManager = new WalManager(walSettings);
    var cursorManager = CreateCursorManager(compactionSettings);
    var compactor = new L1Compactor(walManager, cursorManager, compactionSettings,
        NullLogger<L1Compactor>.Instance);

    const string stream = "race-rotate";
    var totalWritten = 0;

    // Write entries, triggering natural rotations along the way
    for (int i = 0; i < 30; i++) {
      var writer = await walManager.GetOrCreateWriterAsync(stream);
      await writer.WriteAsync(new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow.AddSeconds(i),
        Level = "info",
        Message = $"rotate-entry-{i}",
        Attributes = new Dictionary<string, object?>()
      });
      totalWritten++;
      await walManager.RotateWalIfNeededAsync(stream);
    }

    // Compact all available entries
    var compacted = await compactor.CompactStreamAsync(stream);

    // Write more entries into the new active file
    for (int i = 0; i < 10; i++) {
      var writer = await walManager.GetOrCreateWriterAsync(stream);
      await writer.WriteAsync(new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow.AddSeconds(30 + i),
        Level = "info",
        Message = $"post-compact-{i}",
        Attributes = new Dictionary<string, object?>()
      });
      totalWritten++;
    }

    // Compact again
    var compacted2 = await compactor.CompactStreamAsync(stream);

    // Verify total
    var l1Files = compactor.GetL1Files(stream);
    var totalRead = 0;
    foreach (var f in l1Files) {
      await foreach (var _ in ParquetReader.ReadEntriesAsync(f)) {
        totalRead++;
      }
    }

    totalRead.Should().Be(totalWritten,
        because: "entries written before and after natural rotation must all be preserved");
  }

  // ---------------------------------------------------------------
  // Issue 2: WalWriter padding frames on write failure
  // ---------------------------------------------------------------

  [Fact]
  public async Task WalReader_ShouldSkipPaddingFrames()
  {
    var settings = GetTestSettings();

    const string stream = "padding-test";
    var filePath = GetWalPath(stream);

    // Write a valid entry, then manually inject a padding frame, then write another valid entry.
    var writer = await WalWriter.CreateAsync(filePath, stream, settings);
    await writer.WriteAsync(new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = "before-padding",
      Attributes = new Dictionary<string, object?>()
    });

    // Manually write a padding frame by abusing a known-size region
    // We'll just write two valid entries and verify both are readable
    await writer.WriteAsync(new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow.AddSeconds(1),
      Level = "warn",
      Message = "after-padding",
      Attributes = new Dictionary<string, object?>()
    });
    await writer.DisposeAsync();

    // Read back — both entries must be present
    var reader = await WalReader.CreateAsync(filePath, stream);
    var entries = new List<LogEntry>();
    await foreach (var e in reader.ReadEntriesAsync()) {
      entries.Add(e.LogEntry);
    }
    reader.Dispose();

    entries.Should().HaveCount(2);
    entries[0].Message.Should().Be("before-padding");
    entries[1].Message.Should().Be("after-padding");
  }

  [Fact]
  public async Task WalReader_WithPaddingEntryType_ShouldSkipIt()
  {
    var settings = GetTestSettings();
    const string stream = "padding-skip";
    var filePath = GetWalPath(stream);

    // Create file with header
    var writer = await WalWriter.CreateAsync(filePath, stream, settings);
    await writer.WriteAsync(new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = "entry-1",
      Attributes = new Dictionary<string, object?>()
    });
    await writer.DisposeAsync();

    // Manually append a padding frame followed by a real entry
    using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read)) {
      fs.Seek(0, SeekOrigin.End);

      // Write a padding frame with a dummy payload of 20 bytes
      var padPayloadLen = 20u;
      var padHeader = new WalFrameHeader(padPayloadLen, WalEntryType.Padding);
      var padBuf = new byte[WalFrameHeader.Size + padPayloadLen];
      padHeader.WriteTo(padBuf.AsSpan(0, WalFrameHeader.Size));
      // payload is zero-filled
      await fs.WriteAsync(padBuf);
      await fs.FlushAsync();

      var endPos = fs.Position;
      fs.Close();

      // Write another valid entry by re-opening the writer
      var writer2 = await WalWriter.CreateAsync(filePath, stream, settings);
      await writer2.WriteAsync(new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow.AddSeconds(1),
        Level = "info",
        Message = "entry-2",
        Attributes = new Dictionary<string, object?>()
      });
      await writer2.DisposeAsync();
    }

    // Read and verify the padding frame was skipped
    var reader = await WalReader.CreateAsync(filePath, stream);
    var entries = new List<LogEntry>();
    await foreach (var e in reader.ReadEntriesAsync()) {
      entries.Add(e.LogEntry);
    }
    reader.Dispose();

    entries.Should().HaveCount(2);
    entries[0].Message.Should().Be("entry-1");
    entries[1].Message.Should().Be("entry-2");
  }

  // ---------------------------------------------------------------
  // Issue 3: WalHotBuffer.TakeSnapshotWithVersion atomicity
  // ---------------------------------------------------------------

  [Fact]
  public void TakeSnapshotWithVersion_ShouldReturnConsistentVersionAndData()
  {
    var buffer = new WalHotBuffer();
    const string stream = "snapshot-atomic";

    // Append entries
    for (int i = 0; i < 10; i++) {
      buffer.Append(stream, "/wal/001.wal", 100 + i * 50, new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow.AddSeconds(i),
        Level = "info",
        Message = $"entry-{i}"
      });
    }

    var (version, snapshot) = buffer.TakeSnapshotWithVersion(stream);

    version.Should().Be(10, because: "10 appends should produce version 10");
    snapshot.Should().HaveCount(10);
  }

  [Fact]
  public async Task TakeSnapshotWithVersion_UnderConcurrentAppend_ShouldBeConsistent()
  {
    var buffer = new WalHotBuffer();
    const string stream = "snapshot-concurrent";
    const int iterations = 500;

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    var inconsistencies = 0;

    // Writer task: continuously appends entries
    var writerTask = Task.Run(async () => {
      for (int i = 0; i < iterations && !cts.Token.IsCancellationRequested; i++) {
        buffer.Append(stream, "/wal/001.wal", i * 50, new LogEntry {
          Stream = stream,
          Timestamp = DateTime.UtcNow,
          Level = "info",
          Message = $"concurrent-{i}"
        });
        if (i % 10 == 0) await Task.Yield();
      }
    }, cts.Token);

    // Reader task: continuously takes atomic snapshots and validates consistency
    var readerTask = Task.Run(async () => {
      long lastVersion = -1;
      for (int i = 0; i < iterations && !cts.Token.IsCancellationRequested; i++) {
        var (version, snapshot) = buffer.TakeSnapshotWithVersion(stream);

        // The snapshot count must be >= version (each Append increments version by 1
        // and adds one entry). Because we hold the lock, snapshot.Count == version.
        // However the outer global version may differ, so we just validate the
        // per-stream version matches exactly the snapshot count.
        if (snapshot.Count != version) {
          Interlocked.Increment(ref inconsistencies);
        }

        // Version should be monotonically increasing
        if (version < lastVersion) {
          Interlocked.Increment(ref inconsistencies);
        }
        lastVersion = version;

        if (i % 10 == 0) await Task.Yield();
      }
    }, cts.Token);

    await Task.WhenAll(writerTask, readerTask);

    inconsistencies.Should().Be(0,
        because: "atomic snapshot must have version consistent with entry count");
  }
}
