using FluentAssertions;

using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Wal;

using Xunit;

namespace Lumina.Tests.Storage;

public class WalManagerTests : WalTestBase
{
  [Fact]
  public async Task GetOrCreateWriterAsync_ShouldCreateWriter()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var writer = await manager.GetOrCreateWriterAsync("stream-a");

    writer.Should().NotBeNull();
    writer.Stream.Should().Be("stream-a");
  }

  [Fact]
  public async Task GetOrCreateWriterAsync_SameStream_ShouldReturnSameWriter()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var writer1 = await manager.GetOrCreateWriterAsync("stream-a");
    var writer2 = await manager.GetOrCreateWriterAsync("stream-a");

    writer1.Should().BeSameAs(writer2);
  }

  [Fact]
  public async Task GetOrCreateWriterAsync_DifferentStreams_ShouldReturnDifferentWriters()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var writer1 = await manager.GetOrCreateWriterAsync("stream-a");
    var writer2 = await manager.GetOrCreateWriterAsync("stream-b");

    writer1.Should().NotBeSameAs(writer2);
    writer1.Stream.Should().Be("stream-a");
    writer2.Stream.Should().Be("stream-b");
  }

  [Fact]
  public async Task GetOrCreateWriterAsync_InvalidStreamName_ShouldThrow()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var act = async () => await manager.GetOrCreateWriterAsync("");

    await act.Should().ThrowAsync<ArgumentException>();
  }

  [Fact]
  public async Task GetOrCreateWriterAsync_StreamNameWithInvalidChars_ShouldThrow()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var act = async () => await manager.GetOrCreateWriterAsync("bad/stream");

    await act.Should().ThrowAsync<ArgumentException>();
  }

  [Fact]
  public async Task GetOrCreateWriterAsync_TooLongStreamName_ShouldThrow()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var act = async () => await manager.GetOrCreateWriterAsync(new string('a', 101));

    await act.Should().ThrowAsync<ArgumentException>();
  }

  [Fact]
  public async Task RotateWalIfNeededAsync_ShouldNotRotateWhenUnderLimit()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var writer = await manager.GetOrCreateWriterAsync("test-stream");
    await writer.WriteAsync(CreateTestEntry());

    var rotatedPath = await manager.RotateWalIfNeededAsync("test-stream");

    rotatedPath.Should().BeNull("file hasn't exceeded size limit");
  }

  [Fact]
  public async Task RotateWalIfNeededAsync_ShouldRotateWhenOverLimit()
  {
    var settings = new WalSettings {
      DataDirectory = TempDirectory,
      MaxWalSizeBytes = 100, // Very small
      EnableWriteThrough = false,
      FlushIntervalMs = 100
    };
    await using var manager = new WalManager(settings);

    var writer = await manager.GetOrCreateWriterAsync("test-stream");
    for (int i = 0; i < 20; i++) {
      await writer.WriteAsync(CreateTestEntry(message: new string('x', 100)));
    }

    var rotatedPath = await manager.RotateWalIfNeededAsync("test-stream");

    rotatedPath.Should().NotBeNull();
  }

  [Fact]
  public async Task RotateWalIfNeededAsync_ShouldProvideNewWriter()
  {
    var settings = new WalSettings {
      DataDirectory = TempDirectory,
      MaxWalSizeBytes = 100,
      EnableWriteThrough = false,
      FlushIntervalMs = 100
    };
    await using var manager = new WalManager(settings);

    var firstWriter = await manager.GetOrCreateWriterAsync("test-stream");
    for (int i = 0; i < 20; i++) {
      await firstWriter.WriteAsync(CreateTestEntry(message: new string('x', 100)));
    }

    await manager.RotateWalIfNeededAsync("test-stream");
    var secondWriter = await manager.GetOrCreateWriterAsync("test-stream");

    secondWriter.FilePath.Should().NotBe(firstWriter.FilePath);
  }

  [Fact]
  public async Task GetActiveStreams_ShouldReturnWrittenStreams()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    await (await manager.GetOrCreateWriterAsync("alpha")).WriteAsync(CreateTestEntry(stream: "alpha"));
    await (await manager.GetOrCreateWriterAsync("beta")).WriteAsync(CreateTestEntry(stream: "beta"));

    var streams = manager.GetActiveStreams();

    streams.Should().Contain("alpha");
    streams.Should().Contain("beta");
  }

  [Fact]
  public async Task GetActiveStreams_EmptyDirectory_ShouldReturnEmpty()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var streams = manager.GetActiveStreams();

    streams.Should().BeEmpty();
  }

  [Fact]
  public async Task ReadEntriesAsync_ShouldReadBackAllEntries()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var writer = await manager.GetOrCreateWriterAsync("test-stream");
    await writer.WriteAsync(CreateTestEntry(message: "Entry 1"));
    await writer.WriteAsync(CreateTestEntry(message: "Entry 2"));
    await writer.FlushAsync();

    var entries = new List<LogEntry>();
    await foreach (var entry in manager.ReadEntriesAsync("test-stream")) {
      entries.Add(entry);
    }

    entries.Should().HaveCount(2);
    entries[0].Message.Should().Be("Entry 1");
    entries[1].Message.Should().Be("Entry 2");
  }

  [Fact]
  public async Task GetWalFiles_ShouldReturnCreatedFiles()
  {
    var settings = GetTestSettings();
    await using var manager = new WalManager(settings);

    var writer = await manager.GetOrCreateWriterAsync("test-stream");
    await writer.WriteAsync(CreateTestEntry());

    var files = manager.GetWalFiles("test-stream");

    files.Should().HaveCountGreaterThanOrEqualTo(1);
    files.All(f => f.EndsWith(".wal")).Should().BeTrue();
  }

  [Fact]
  public async Task DisposeAsync_ShouldFlushAndCloseAllWriters()
  {
    var settings = GetTestSettings();
    var manager = new WalManager(settings);

    var writer = await manager.GetOrCreateWriterAsync("test-stream");
    await writer.WriteAsync(CreateTestEntry());

    await manager.DisposeAsync();

    // Verify the file still exists and is valid
    var files = Directory.GetFiles(
        Path.Combine(TempDirectory, "test-stream"), "*.wal");
    files.Should().NotBeEmpty();
  }

  [Fact]
  public async Task DisposeAsync_CalledTwice_ShouldNotThrow()
  {
    var settings = GetTestSettings();
    var manager = new WalManager(settings);
    await manager.GetOrCreateWriterAsync("test-stream");

    await manager.DisposeAsync();
    var act = async () => await manager.DisposeAsync();

    await act.Should().NotThrowAsync();
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
