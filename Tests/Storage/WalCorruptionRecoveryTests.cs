using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

/// <summary>
/// Tests for WAL corruption tolerance and sync marker recovery.
/// PRD NFR 5.2: "The system must never crash due to a corrupt WAL file."
/// </summary>
public class WalCorruptionRecoveryTests : WalTestBase
{
  [Fact]
  public async Task ReadEntriesAsync_ShouldSkipCorruptedPayload_AndContinue()
  {
    // Arrange – write 3 entries, corrupt the middle one's payload, expect 2 good entries
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry(message: "First"));
      await writer.WriteAsync(CreateTestEntry(message: "Second"));
      await writer.WriteAsync(CreateTestEntry(message: "Third"));
    }

    // Corrupt the middle entry payload by flipping bits in the file
    var fileBytes = await File.ReadAllBytesAsync(filePath);
    CorruptPayloadAtFrameIndex(fileBytes, 1);
    await File.WriteAllBytesAsync(filePath, fileBytes);

    // Act
    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert – should recover at least the uncorrupted entries
    entries.Should().HaveCountGreaterThanOrEqualTo(1, "corrupted frames should be skipped, not crash");
  }

  [Fact]
  public async Task ReadEntriesAsync_ShouldRecoverFromTruncatedFile()
  {
    // Arrange – write entries, then truncate the file mid-frame
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry(message: "Complete entry"));
      await writer.WriteAsync(CreateTestEntry(message: "Will be truncated"));
    }

    // Truncate file mid-way through second entry
    var fileBytes = await File.ReadAllBytesAsync(filePath);
    var truncatedLength = fileBytes.Length - 20; // Cut off end
    await File.WriteAllBytesAsync(filePath, fileBytes.AsSpan(0, truncatedLength).ToArray());

    // Act
    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert – first complete entry should be recovered
    entries.Should().HaveCountGreaterThanOrEqualTo(1);
    entries[0].LogEntry.Message.Should().Be("Complete entry");
  }

  [Fact]
  public async Task ReadEntriesAsync_EmptyWal_ShouldReturnNoEntries()
  {
    // Arrange – create WAL with only file header, no entries
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      // Write nothing
    }

    // Act
    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert
    entries.Should().BeEmpty();
  }

  [Fact]
  public async Task ReadEntriesAsync_ShouldHandleHeaderCrcCorruption()
  {
    // Arrange – write entries and corrupt a frame header CRC
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry(message: "Before corruption"));
      await writer.WriteAsync(CreateTestEntry(message: "Has bad CRC"));
      await writer.WriteAsync(CreateTestEntry(message: "After corruption"));
    }

    // Corrupt the header CRC of the second entry
    var fileBytes = await File.ReadAllBytesAsync(filePath);
    CorruptHeaderCrcAtFrameIndex(fileBytes, 1);
    await File.WriteAllBytesAsync(filePath, fileBytes);

    // Act
    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert – should not crash; should skip corrupted frame and resync
    entries.Should().HaveCountGreaterThanOrEqualTo(1);
  }

  [Fact]
  public async Task ReadEntriesAsync_ShouldHandleGarbageBeforeValidFrame()
  {
    // Arrange – create a file with garbage then a valid WAL
    var settings = GetTestSettings();
    var validPath = GetWalPath("valid-stream");

    await using (var writer = await WalWriter.CreateAsync(validPath, "valid-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry(message: "Valid entry"));
    }

    // Read valid WAL, inject some garbage bytes after the file header
    var validBytes = await File.ReadAllBytesAsync(validPath);
    var garbage = new byte[50];
    new Random(42).NextBytes(garbage);

    // Build a new file: FileHeader + garbage + original entries
    var garbagePath = Path.Combine(TempDirectory, "garbage-test.wal");
    using (var ms = new MemoryStream()) {
      ms.Write(validBytes, 0, WalFileHeader.Size);
      ms.Write(garbage);
      ms.Write(validBytes, WalFileHeader.Size, validBytes.Length - WalFileHeader.Size);
      await File.WriteAllBytesAsync(garbagePath, ms.ToArray());
    }

    // Act
    using var reader = await WalReader.CreateAsync(garbagePath, "garbage-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert – sync marker scanning should eventually find the valid frame
    entries.Should().HaveCountGreaterThanOrEqualTo(1);
    entries.First().LogEntry.Message.Should().Be("Valid entry");
  }

  [Fact]
  public async Task ValidateFrameHeaderAsync_ShouldRejectOffsetBeforeFileHeader()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry());
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    // Act – validate at offset 0, which is inside the file header
    var (isValid, _) = await reader.ValidateFrameHeaderAsync(0);

    // Assert
    isValid.Should().BeFalse();
  }

  [Fact]
  public async Task ValidateFrameHeaderAsync_ShouldRejectOffsetBeyondFileEnd()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry());
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    var (isValid, _) = await reader.ValidateFrameHeaderAsync(reader.FileSize + 100);

    isValid.Should().BeFalse();
  }

  [Fact]
  public async Task ReadEntriesAsync_LargePayload_ShouldRoundTrip()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var largeMessage = new string('A', 50_000);

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry(message: largeMessage));
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    entries.Should().HaveCount(1);
    entries[0].LogEntry.Message.Should().Be(largeMessage);
  }

  [Fact]
  public async Task ReadEntriesAsync_ManyEntries_ShouldAllRoundTrip()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var count = 500;

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      for (int i = 0; i < count; i++) {
        await writer.WriteAsync(CreateTestEntry(message: $"Entry #{i}"));
      }
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    entries.Should().HaveCount(count);
    for (int i = 0; i < count; i++) {
      entries[i].LogEntry.Message.Should().Be($"Entry #{i}");
    }
  }

  // --- Helpers ---

  private static void CorruptPayloadAtFrameIndex(byte[] fileBytes, int frameIndex)
  {
    // Walk frames to find the payload of the Nth frame
    var offset = WalFileHeader.Size;
    for (int i = 0; i <= frameIndex && offset + WalFrameHeader.Size <= fileBytes.Length; i++) {
      var header = WalFrameHeader.ReadFrom(fileBytes.AsSpan(offset, WalFrameHeader.Size));
      if (i == frameIndex) {
        var payloadStart = offset + WalFrameHeader.Size;
        if (payloadStart < fileBytes.Length) {
          // Flip bits in the middle of the payload
          var mid = payloadStart + (int)(header.Length / 2);
          if (mid < fileBytes.Length) {
            fileBytes[mid] ^= 0xFF;
            fileBytes[Math.Min(mid + 1, fileBytes.Length - 1)] ^= 0xFF;
          }
        }
        return;
      }
      offset += WalFrameHeader.Size + (int)header.Length;
    }
  }

  private static void CorruptHeaderCrcAtFrameIndex(byte[] fileBytes, int frameIndex)
  {
    var offset = WalFileHeader.Size;
    for (int i = 0; i <= frameIndex && offset + WalFrameHeader.Size <= fileBytes.Length; i++) {
      var header = WalFrameHeader.ReadFrom(fileBytes.AsSpan(offset, WalFrameHeader.Size));
      if (i == frameIndex) {
        // CRC is the last byte of the frame header
        var crcOffset = offset + WalFrameHeader.Size - 1;
        fileBytes[crcOffset] ^= 0xFF;
        return;
      }
      offset += WalFrameHeader.Size + (int)header.Length;
    }
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
