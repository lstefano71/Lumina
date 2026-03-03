using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Wal;

using Xunit;
using Xunit.Abstractions;

namespace Lumina.Tests.Storage;

/// <summary>
/// Fuzz tests for WAL corruption resilience.
/// PRD Phase 4: "Intentionally corrupt WAL files (random bit flips) and verify WalReader recovery."
/// </summary>
public class WalFuzzTests : WalTestBase
{
  private readonly ITestOutputHelper _output;

  public WalFuzzTests(ITestOutputHelper output)
  {
    _output = output;
  }

  [Theory]
  [InlineData(1)]
  [InlineData(5)]
  [InlineData(20)]
  [InlineData(100)]
  public async Task RandomBitFlips_ShouldNeverCrashReader(int flips)
  {
    // Arrange – write a valid WAL
    var settings = GetTestSettings();
    var filePath = GetWalPath("fuzz-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "fuzz-stream", settings)) {
      for (int i = 0; i < 10; i++) {
        await writer.WriteAsync(CreateTestEntry(message: $"Fuzz entry {i}"));
      }
    }

    // Corrupt with random bit flips in the data area (after file header)
    var fileBytes = await File.ReadAllBytesAsync(filePath);
    var rng = new Random(flips * 42);
    for (int f = 0; f < flips; f++) {
      var pos = rng.Next(WalFileHeader.Size, fileBytes.Length);
      var bit = 1 << rng.Next(8);
      fileBytes[pos] ^= (byte)bit;
    }
    await File.WriteAllBytesAsync(filePath, fileBytes);

    // Act – reader must not throw
    var act = async () => {
      using var reader = await WalReader.CreateAsync(filePath, "fuzz-stream");
      var entries = await reader.ReadEntriesAsync().ToListAsync();
      _output.WriteLine($"Flips={flips}, recovered {entries.Count} entries");
    };

    await act.Should().NotThrowAsync(
        $"WalReader must tolerate {flips} random bit flip(s) without crashing");
  }

  [Fact]
  public async Task TruncatedToHeaderOnly_ShouldNotCrash()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("truncated-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "truncated-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry());
    }

    // Truncate to just the file header
    var headerBytes = new byte[WalFileHeader.Size];
    Array.Copy(await File.ReadAllBytesAsync(filePath), headerBytes, WalFileHeader.Size);
    await File.WriteAllBytesAsync(filePath, headerBytes);

    // Act
    using var reader = await WalReader.CreateAsync(filePath, "truncated-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    entries.Should().BeEmpty();
  }

  [Fact]
  public async Task AllZeroPayloadArea_ShouldNotCrash()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("zero-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "zero-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry());
    }

    // Zero out everything after the file header
    var fileBytes = await File.ReadAllBytesAsync(filePath);
    Array.Clear(fileBytes, WalFileHeader.Size, fileBytes.Length - WalFileHeader.Size);
    await File.WriteAllBytesAsync(filePath, fileBytes);

    var act = async () => {
      using var reader = await WalReader.CreateAsync(filePath, "zero-stream");
      await reader.ReadEntriesAsync().ToListAsync();
    };

    await act.Should().NotThrowAsync();
  }

  [Fact]
  public async Task AllOnesPayloadArea_ShouldNotCrash()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("ones-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "ones-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry());
    }

    var fileBytes = await File.ReadAllBytesAsync(filePath);
    for (int i = WalFileHeader.Size; i < fileBytes.Length; i++) {
      fileBytes[i] = 0xFF;
    }
    await File.WriteAllBytesAsync(filePath, fileBytes);

    var act = async () => {
      using var reader = await WalReader.CreateAsync(filePath, "ones-stream");
      await reader.ReadEntriesAsync().ToListAsync();
    };

    await act.Should().NotThrowAsync();
  }

  [Fact]
  public async Task RandomData_AppendedAfterValidEntries_ShouldRecoverValid()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("append-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "append-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry(message: "Valid entry"));
    }

    // Append random garbage to the end
    var garbage = new byte[500];
    new Random(99).NextBytes(garbage);
    await using (var fs = new FileStream(filePath, FileMode.Append, FileAccess.Write)) {
      await fs.WriteAsync(garbage);
    }

    using var reader = await WalReader.CreateAsync(filePath, "append-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    entries.Should().HaveCountGreaterThanOrEqualTo(1);
    entries[0].LogEntry.Message.Should().Be("Valid entry");
  }

  [Fact]
  public async Task SyncMarkerInsidePayload_ShouldNotConfuseReader()
  {
    // Embed the sync marker bytes inside a message to ensure the reader
    // doesn't falsely detect a new frame boundary
    var settings = GetTestSettings();
    var filePath = GetWalPath("marker-stream");

    // Build a message that embeds the sync marker pattern
    var syncBytes = new byte[] { 0xFA, 0xCE, 0xB0, 0x0C };
    var trickMessage = "Before" + System.Text.Encoding.ASCII.GetString(syncBytes) + "After";

    await using (var writer = await WalWriter.CreateAsync(filePath, "marker-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry(message: "Normal entry"));
      await writer.WriteAsync(CreateTestEntry(message: trickMessage));
      await writer.WriteAsync(CreateTestEntry(message: "Final entry"));
    }

    using var reader = await WalReader.CreateAsync(filePath, "marker-stream");
    var entries = await reader.ReadEntriesAsync().ToListAsync();

    // The WAL payload is MessagePack-serialized, so the raw sync marker bytes
    // inside the message won't match the frame pattern. All 3 entries should be read.
    entries.Should().HaveCount(3);
  }

  [Theory]
  [InlineData(1)]
  [InlineData(10)]
  [InlineData(50)]
  public async Task SeveralFrames_RandomByteOverwrite_ShouldRecoverSubset(int overwrites)
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("overwrite-stream");
    var entryCount = 20;

    await using (var writer = await WalWriter.CreateAsync(filePath, "overwrite-stream", settings)) {
      for (int i = 0; i < entryCount; i++) {
        await writer.WriteAsync(CreateTestEntry(message: $"Entry {i}"));
      }
    }

    // Overwrite random single bytes (not in file header)
    var fileBytes = await File.ReadAllBytesAsync(filePath);
    var rng = new Random(overwrites * 7);
    for (int o = 0; o < overwrites; o++) {
      var pos = rng.Next(WalFileHeader.Size, fileBytes.Length);
      fileBytes[pos] = (byte)rng.Next(256);
    }
    await File.WriteAllBytesAsync(filePath, fileBytes);

    // Must not crash
    var act = async () => {
      using var reader = await WalReader.CreateAsync(filePath, "overwrite-stream");
      var entries = await reader.ReadEntriesAsync().ToListAsync();
      _output.WriteLine($"Overwrites={overwrites}, recovered {entries.Count}/{entryCount}");
    };

    await act.Should().NotThrowAsync();
  }

  [Fact]
  public async Task FileHeaderCorrupted_ShouldThrowOnOpen()
  {
    var settings = GetTestSettings();
    var filePath = GetWalPath("bad-header-stream");

    await using (var writer = await WalWriter.CreateAsync(filePath, "bad-header-stream", settings)) {
      await writer.WriteAsync(CreateTestEntry());
    }

    // Corrupt the magic bytes
    var fileBytes = await File.ReadAllBytesAsync(filePath);
    fileBytes[0] = 0x00;
    fileBytes[1] = 0x00;
    await File.WriteAllBytesAsync(filePath, fileBytes);

    // Reader should fail to open since the file header is invalid
    var act = async () => await WalReader.CreateAsync(filePath, "bad-header-stream");

    await act.Should().ThrowAsync<Exception>();
  }

  private static LogEntry CreateTestEntry(string stream = "fuzz-stream", string message = "Fuzz test")
  {
    return new LogEntry {
      Stream = stream,
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = message,
      Attributes = new Dictionary<string, object?> {
        ["key"] = "value"
      }
    };
  }
}
