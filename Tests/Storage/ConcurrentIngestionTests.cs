using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

/// <summary>
/// Concurrent ingestion tests verifying WAL integrity under parallel writes.
/// </summary>
public class ConcurrentIngestionTests : WalTestBase
{
  [Fact]
  public async Task ConcurrentWrites_SameStream_ShouldPreserveAllEntries()
  {
    var settings = GetTestSettings();
    await using var walManager = new WalManager(settings);

    const string stream = "concurrent-same";
    const int writersCount = 4;
    const int entriesPerWriter = 50;

    var tasks = Enumerable.Range(0, writersCount).Select(async w => {
      for (int i = 0; i < entriesPerWriter; i++) {
        var writer = await walManager.GetOrCreateWriterAsync(stream);
        await writer.WriteAsync(new LogEntry {
          Stream = stream,
          Timestamp = DateTime.UtcNow,
          Level = "info",
          Message = $"writer-{w}-entry-{i}",
          Attributes = new Dictionary<string, object?>()
        });
      }
    });

    await Task.WhenAll(tasks);

    // Read all entries back
    var readEntries = new List<LogEntry>();
    await foreach (var entry in walManager.ReadEntriesAsync(stream)) {
      readEntries.Add(entry);
    }

    readEntries.Should().HaveCount(writersCount * entriesPerWriter);
  }

  [Fact]
  public async Task ConcurrentWrites_DifferentStreams_ShouldIsolateData()
  {
    var settings = GetTestSettings();
    await using var walManager = new WalManager(settings);

    const int streamCount = 4;
    const int entriesPerStream = 50;

    var tasks = Enumerable.Range(0, streamCount).Select(async s => {
      var stream = $"stream-{s}";
      for (int i = 0; i < entriesPerStream; i++) {
        var writer = await walManager.GetOrCreateWriterAsync(stream);
        await writer.WriteAsync(new LogEntry {
          Stream = stream,
          Timestamp = DateTime.UtcNow,
          Level = "info",
          Message = $"stream-{s}-entry-{i}",
          Attributes = new Dictionary<string, object?>()
        });
      }
    });

    await Task.WhenAll(tasks);

    // Each stream should have exactly entriesPerStream entries
    for (int s = 0; s < streamCount; s++) {
      var stream = $"stream-{s}";
      var entries = new List<LogEntry>();
      await foreach (var entry in walManager.ReadEntriesAsync(stream)) {
        entries.Add(entry);
      }
      entries.Should().HaveCount(entriesPerStream,
          because: $"stream-{s} should have all its entries isolated");
    }
  }

  [Fact]
  public async Task ConcurrentBatchWrites_ShouldPreserveAllEntries()
  {
    var settings = GetTestSettings();
    await using var walManager = new WalManager(settings);

    const string stream = "concurrent-batch";
    const int batchCount = 8;
    const int batchSize = 25;

    var tasks = Enumerable.Range(0, batchCount).Select(async b => {
      var entries = Enumerable.Range(0, batchSize).Select(i => new LogEntry {
        Stream = stream,
        Timestamp = DateTime.UtcNow,
        Level = "info",
        Message = $"batch-{b}-entry-{i}",
        Attributes = new Dictionary<string, object?>()
      }).ToList();

      var writer = await walManager.GetOrCreateWriterAsync(stream);
      await writer.WriteBatchAsync(entries);
    });

    await Task.WhenAll(tasks);

    var readEntries = new List<LogEntry>();
    await foreach (var entry in walManager.ReadEntriesAsync(stream)) {
      readEntries.Add(entry);
    }

    readEntries.Should().HaveCount(batchCount * batchSize);
  }

  [Fact]
  public async Task ConcurrentWrites_WithRotation_ShouldNotLoseData()
  {
    var settings = new WalSettings {
      DataDirectory = TempDirectory,
      MaxWalSizeBytes = 4096, // small size to trigger rotation
      EnableWriteThrough = false,
      FlushIntervalMs = 100
    };
    await using var walManager = new WalManager(settings);

    const string stream = "concurrent-rotate";
    const int writerCount = 4;
    const int entriesPerWriter = 30;

    var tasks = Enumerable.Range(0, writerCount).Select(async w => {
      for (int i = 0; i < entriesPerWriter; i++) {
        var writer = await walManager.GetOrCreateWriterAsync(stream);
        await writer.WriteAsync(new LogEntry {
          Stream = stream,
          Timestamp = DateTime.UtcNow,
          Level = "info",
          Message = $"rotate-w{w}-e{i}",
          Attributes = new Dictionary<string, object?>()
        });
        await walManager.RotateWalIfNeededAsync(stream);
      }
    });

    await Task.WhenAll(tasks);

    var readEntries = new List<LogEntry>();
    await foreach (var entry in walManager.ReadEntriesAsync(stream)) {
      readEntries.Add(entry);
    }

    // Under rotation and concurrency some entries may land in rotated files;
    // the critical invariant is zero data loss.
    readEntries.Count.Should().Be(writerCount * entriesPerWriter);
  }
}
