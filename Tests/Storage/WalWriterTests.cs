using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

public class WalWriterTests : WalTestBase
{
  [Fact]
  public async Task CreateAsync_ShouldCreateFileWithValidHeader()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    // Act
    await using var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings);

    // Assert
    File.Exists(filePath).Should().BeTrue();
    writer.FilePath.Should().Be(filePath);
    writer.Stream.Should().Be("test-stream");
  }

  [Fact]
  public async Task WriteAsync_ShouldWriteSingleEntry()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entry = CreateTestEntry();

    await using var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings);

    // Act
    var offset = await writer.WriteAsync(entry);

    // Assert
    offset.Should().BeGreaterOrEqualTo(WalFileHeader.Size);
    writer.FileSize.Should().BeGreaterThan(WalFileHeader.Size);
  }

  [Fact]
  public async Task WriteBatchAsync_ShouldWriteMultipleEntries()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entries = new[]
    {
            CreateTestEntry(message: "Message 1"),
            CreateTestEntry(message: "Message 2"),
            CreateTestEntry(message: "Message 3")
        };

    await using var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings);

    // Act
    var offsets = await writer.WriteBatchAsync(entries);

    // Assert
    offsets.Should().HaveCount(3);
    offsets[0].Should().BeGreaterOrEqualTo(WalFileHeader.Size);
    offsets[1].Should().BeGreaterThan(offsets[0]);
    offsets[2].Should().BeGreaterThan(offsets[1]);
  }

  [Fact]
  public async Task WriteAsync_ShouldPersistToFile()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entry = CreateTestEntry();

    // Act - Write and dispose
    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(entry);
    }

    // Assert - File should exist with content
    File.Exists(filePath).Should().BeTrue();
    var fileInfo = new FileInfo(filePath);
    fileInfo.Length.Should().BeGreaterThan(WalFileHeader.Size);
  }

  [Fact]
  public async Task FlushAsync_ShouldNotThrow()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entry = CreateTestEntry();

    await using var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings);
    await writer.WriteAsync(entry);

    // Act
    var act = async () => await writer.FlushAsync();

    // Assert
    await act.Should().NotThrowAsync();
  }

  [Fact]
  public async Task NeedsRotation_ShouldReturnTrue_WhenSizeExceeded()
  {
    // Arrange
    var settings = new WalSettings {
      DataDirectory = TempDirectory,
      MaxWalSizeBytes = 100, // Very small limit
      EnableWriteThrough = false,
      FlushIntervalMs = 100
    };
    var filePath = GetWalPath("test-stream");

    await using var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings);

    // Write enough entries to exceed limit
    for (int i = 0; i < 20; i++) {
      await writer.WriteAsync(CreateTestEntry(message: new string('x', 100)));
    }

    // Act
    var needsRotation = writer.NeedsRotation();

    // Assert
    needsRotation.Should().BeTrue();
  }

  [Fact]
  public async Task Entries_ShouldSurviveWriterDispose_WithoutWriteThrough()
  {
    // Regression test for data loss on restart when EnableWriteThrough=false.
    // Writes via RandomAccess bypass the FileStream managed buffer, so
    // FlushAsync() (which calls Flush(flushToDisk:false)) was a no-op.
    // After the fix, DisposeAsync() calls Flush(flushToDisk:true) which
    // invokes FlushFileBuffers, ensuring data reaches physical storage.

    var settings = new WalSettings {
      DataDirectory = TempDirectory,
      MaxWalSizeBytes = 10 * 1024 * 1024,
      EnableWriteThrough = false, // the problematic path
      FlushIntervalMs = 100
    };
    var filePath = GetWalPath("test-stream");
    const int entryCount = 927; // matches the reproduction count

    // --- Write phase: create writer, write entries, dispose ---
    {
      await using var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings);
      for (int i = 0; i < entryCount; i++) {
        await writer.WriteAsync(CreateTestEntry(message: $"msg #{i}"));
      }
    } // writer disposed here — SyncToDisk() should be called

    // --- Read phase: open a fresh reader and count entries ---
    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var count = 0;
    await foreach (var entry in reader.ReadEntriesAsync()) {
      count++;
    }

    count.Should().Be(entryCount,
        "all entries must survive a writer dispose cycle when WriteThrough is disabled");
  }

  [Fact]
  public async Task Entries_ShouldSurviveBatchWriterDispose_WithoutWriteThrough()
  {
    var settings = new WalSettings {
      DataDirectory = TempDirectory,
      MaxWalSizeBytes = 10 * 1024 * 1024,
      EnableWriteThrough = false,
      FlushIntervalMs = 100
    };
    var filePath = GetWalPath("test-stream");
    const int batchSize = 1000;

    // --- Write phase ---
    {
      await using var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings);
      var batch = Enumerable.Range(0, batchSize)
          .Select(i => CreateTestEntry(message: $"batch msg #{i}"))
          .ToList();
      await writer.WriteBatchAsync(batch);
    }

    // --- Read phase ---
    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var count = 0;
    await foreach (var _ in reader.ReadEntriesAsync()) {
      count++;
    }

    count.Should().Be(batchSize,
        "all batch entries must survive a writer dispose cycle when WriteThrough is disabled");
  }

  private static LogEntry CreateTestEntry(string stream = "test-stream", string message = "Test message")
  {
    return new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = message,
      Attributes = new Dictionary<string, object?> {
        ["key1"] = "value1",
        ["key2"] = 42
      }
    };
  }
}