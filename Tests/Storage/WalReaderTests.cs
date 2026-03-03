using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

public class WalReaderTests : WalTestBase
{
  [Fact]
  public async Task CreateAsync_ShouldOpenExistingFile()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entry = CreateTestEntry();

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(entry);
    }

    // Act
    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    // Assert
    reader.FilePath.Should().Be(filePath);
    reader.Stream.Should().Be("test-stream");
    reader.FileSize.Should().BeGreaterThan(0);
  }

  [Fact]
  public async Task ReadEntriesAsync_ShouldReadSingleEntry()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entry = CreateTestEntry();

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(entry);
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    // Act
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert
    entries.Should().HaveCount(1);
    entries[0].LogEntry.Stream.Should().Be(entry.Stream);
    entries[0].LogEntry.Message.Should().Be(entry.Message);
    entries[0].LogEntry.Level.Should().Be(entry.Level);
  }

  [Fact]
  public async Task ReadEntriesAsync_ShouldReadMultipleEntries()
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

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteBatchAsync(entries);
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    // Act
    var readEntries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert
    readEntries.Should().HaveCount(3);
    readEntries[0].LogEntry.Message.Should().Be("Message 1");
    readEntries[1].LogEntry.Message.Should().Be("Message 2");
    readEntries[2].LogEntry.Message.Should().Be("Message 3");
  }

  [Fact]
  public async Task ReadEntriesAsync_ShouldPreserveAttributes()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entry = new LogEntry {
      Stream = "test-stream",
      Timestamp = DateTime.UtcNow,
      Level = "error",
      Message = "Test message with attributes",
      Attributes = new Dictionary<string, object?> {
        ["string_key"] = "string_value",
        ["int_key"] = 42,
        ["bool_key"] = true,
        ["nested_key"] = new Dictionary<string, object?> { ["inner"] = "value" }
      },
      TraceId = "trace-123",
      SpanId = "span-456",
      DurationMs = 150
    };

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(entry);
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    // Act
    var readEntries = await reader.ReadEntriesAsync().ToListAsync();

    // Assert
    readEntries.Should().HaveCount(1);
    var readEntry = readEntries[0].LogEntry;
    readEntry.Attributes.Should().ContainKey("string_key");
    readEntry.Attributes["string_key"].Should().Be("string_value");
    readEntry.Attributes["int_key"].Should().Be(42);
    readEntry.TraceId.Should().Be("trace-123");
    readEntry.SpanId.Should().Be("span-456");
    readEntry.DurationMs.Should().Be(150);
  }

  [Fact]
  public async Task ReadFileHeaderAsync_ShouldReturnValidHeader()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry());
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    // Act
    var header = await reader.ReadFileHeaderAsync();

    // Assert
    header.IsValid.Should().BeTrue();
  }

  [Fact]
  public async Task ValidateFrameHeaderAsync_ShouldReturnTrueForValidOffset()
  {
    // Arrange
    var settings = GetTestSettings();
    var filePath = GetWalPath("test-stream");
    var entry = CreateTestEntry();

    long offset;
    await using (var writer = await WalWriter.CreateAsync(filePath, "test-stream", settings)) {
      offset = await writer.WriteAsync(entry);
    }

    using var reader = await WalReader.CreateAsync(filePath, "test-stream");

    // Act
    var (isValid, header) = await reader.ValidateFrameHeaderAsync(offset);

    // Assert
    isValid.Should().BeTrue();
    header.IsValid.Should().BeTrue();
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