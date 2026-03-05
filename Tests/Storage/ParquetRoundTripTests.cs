using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Parquet;

using Xunit;

namespace Lumina.Tests.Storage;

public class ParquetRoundTripTests : WalTestBase
{
  [Fact]
  public async Task WriteBatchAsync_ReadEntriesAsync_ShouldRoundTrip()
  {
    var outputPath = Path.Combine(TempDirectory, "roundtrip.parquet");
    var entries = new[]
    {
            CreateEntry("msg-1", "info"),
            CreateEntry("msg-2", "error"),
            CreateEntry("msg-3", "debug")
        };

    await ParquetWriter.WriteBatchAsync(entries, outputPath);

    var read = await ParquetReader.ReadEntriesAsync(outputPath).ToListAsync();

    read.Should().HaveCount(3);
    read.Select(e => e.Message).Should().BeEquivalentTo(new[] { "msg-1", "msg-2", "msg-3" });
  }

  [Fact]
  public async Task WriteBatchAsync_ShouldPreserveFixedFields()
  {
    var outputPath = Path.Combine(TempDirectory, "fixed.parquet");
    var ts = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc);
    var entry = new LogEntry {
      Stream = "my-stream",
      Timestamp = ts,
      Level = "warn",
      Message = "Something happened",
      TraceId = "trace-abc",
      SpanId = "span-xyz",
      DurationMs = 250,
      Attributes = new Dictionary<string, object?>()
    };

    await ParquetWriter.WriteBatchAsync(new[] { entry }, outputPath);
    var read = (await ParquetReader.ReadEntriesAsync(outputPath).ToListAsync()).Single();

    read.Stream.Should().Be("my-stream");
    read.Timestamp.Should().BeCloseTo(ts, TimeSpan.FromSeconds(1));
    read.Level.Should().Be("warn");
    read.Message.Should().Be("Something happened");
    read.TraceId.Should().Be("trace-abc");
    read.SpanId.Should().Be("span-xyz");
    read.DurationMs.Should().Be(250);
  }

  [Fact]
  public async Task WriteBatchAsync_ShouldPreserveStringAttributes()
  {
    var outputPath = Path.Combine(TempDirectory, "str_attrs.parquet");
    var entries = Enumerable.Range(0, 20).Select(i =>
        new LogEntry {
          Stream = "s",
          Timestamp = DateTime.UtcNow,
          Level = "info",
          Message = $"m{i}",
          Attributes = new Dictionary<string, object?> { ["env"] = "production" }
        }).ToArray();

    await ParquetWriter.WriteBatchAsync(entries, outputPath);
    var read = await ParquetReader.ReadEntriesAsync(outputPath).ToListAsync();

    read.Should().HaveCount(20);
    read.All(e => e.Attributes.ContainsKey("env") && (string?)e.Attributes["env"] == "production")
        .Should().BeTrue();
  }

  [Fact]
  public async Task WriteBatchAsync_ShouldPreserveIntAttributes()
  {
    var outputPath = Path.Combine(TempDirectory, "int_attrs.parquet");
    var entries = Enumerable.Range(0, 20).Select(i =>
        new LogEntry {
          Stream = "s",
          Timestamp = DateTime.UtcNow,
          Level = "info",
          Message = "m",
          Attributes = new Dictionary<string, object?> { ["status"] = 200 }
        }).ToArray();

    await ParquetWriter.WriteBatchAsync(entries, outputPath);
    var read = await ParquetReader.ReadEntriesAsync(outputPath).ToListAsync();

    read.Should().HaveCount(20);
    read.All(e => (int?)e.Attributes["status"] == 200).Should().BeTrue();
  }

  [Fact]
  public async Task WriteBatchAsync_ShouldHandleNullableAttributes()
  {
    var outputPath = Path.Combine(TempDirectory, "nullable.parquet");
    var entries = new List<LogEntry>();
    for (int i = 0; i < 20; i++) {
      var attrs = new Dictionary<string, object?> { ["always"] = "present" };
      if (i % 2 == 0) attrs["optional"] = "here";
      entries.Add(new LogEntry {
        Stream = "s",
        Timestamp = DateTime.UtcNow,
        Level = "info",
        Message = "m",
        Attributes = attrs
      });
    }

    await ParquetWriter.WriteBatchAsync(entries, outputPath);
    var read = await ParquetReader.ReadEntriesAsync(outputPath).ToListAsync();

    read.Should().HaveCount(20);
  }

  [Fact]
  public async Task WriteBatchAsync_EmptyEntries_ShouldThrow()
  {
    var outputPath = Path.Combine(TempDirectory, "empty.parquet");

    var act = async () => await ParquetWriter.WriteBatchAsync(
        Array.Empty<LogEntry>(), outputPath);

    await act.Should().ThrowAsync<ArgumentException>();
  }

  [Fact]
  public async Task WriteBatchAsync_ShouldCreateOutputDirectory()
  {
    var nestedPath = Path.Combine(TempDirectory, "nested", "deep", "output.parquet");

    var entries = new[] { CreateEntry() };
    await ParquetWriter.WriteBatchAsync(entries, nestedPath);

    File.Exists(nestedPath).Should().BeTrue();
  }

  [Fact]
  public void GenerateFileName_ShouldIncludeStreamAndTimestamps()
  {
    var start = new DateTime(2025, 1, 15, 10, 0, 0, DateTimeKind.Utc);
    var end = new DateTime(2025, 1, 15, 10, 10, 0, DateTimeKind.Utc);

    var name = ParquetWriter.GenerateFileName("my-stream", start, end);

    name.Should().Contain("my-stream");
    name.Should().EndWith(".parquet");
    name.Should().Contain("20250115_100000");
    name.Should().Contain("20250115_101000");
  }

  [Fact]
  public void GenerateFileName_WithHash_ShouldIncludeHash()
  {
    var start = new DateTime(2025, 1, 15, 10, 0, 0, DateTimeKind.Utc);
    var end = new DateTime(2025, 1, 15, 10, 10, 0, DateTimeKind.Utc);

    var name = ParquetWriter.GenerateFileName("s", start, end, "abc123");

    name.Should().Contain("abc123");
    name.Should().EndWith(".parquet");
  }

  [Fact]
  public async Task WriteBatchAsync_LargeBatch_ShouldSucceed()
  {
    var outputPath = Path.Combine(TempDirectory, "large.parquet");
    var entries = Enumerable.Range(0, 1000).Select(i =>
        CreateEntry($"Message {i}")).ToArray();

    await ParquetWriter.WriteBatchAsync(entries, outputPath);
    var read = await ParquetReader.ReadEntriesAsync(outputPath).ToListAsync();

    read.Should().HaveCount(1000);
  }

  [Fact]
  public async Task WriteBatchAsync_ShouldHandleMixedTypeAttributes()
  {
    var outputPath = Path.Combine(TempDirectory, "mixed.parquet");
    var entries = Enumerable.Range(0, 20).Select(i =>
        new LogEntry {
          Stream = "s",
          Timestamp = DateTime.UtcNow,
          Level = "info",
          Message = "m",
          Attributes = new Dictionary<string, object?> {
            ["str"] = "text",
            ["num"] = 42,
            ["flag"] = true,
            ["pi"] = 3.14
          }
        }).ToArray();

    await ParquetWriter.WriteBatchAsync(entries, outputPath);
    var read = await ParquetReader.ReadEntriesAsync(outputPath).ToListAsync();

    read.Should().HaveCount(20);
  }

  [Fact]
  public async Task ReadEntriesAsync_NonExistentFile_ShouldThrow()
  {
    var act = async () => {
      await foreach (var _ in ParquetReader.ReadEntriesAsync(
          Path.Combine(TempDirectory, "nonexistent.parquet"))) {
      }
    };

    await act.Should().ThrowAsync<Exception>();
  }

  [Fact]
  public async Task WriteBatchAsync_TimestampColumnStats_ShouldMatchDataRange()
  {
    var outputPath = Path.Combine(TempDirectory, "stats_ts.parquet");
    var t1 = new DateTime(2026, 3, 5, 14, 21, 50, DateTimeKind.Utc).AddTicks(1230);
    var t2 = t1.AddSeconds(10);
    var t3 = t1.AddSeconds(20);

    var entries = new[] {
      new LogEntry { Stream = "test-stream", Timestamp = t2, Level = "info", Message = "b", Attributes = new Dictionary<string, object?>() },
      new LogEntry { Stream = "test-stream", Timestamp = t1, Level = "info", Message = "a", Attributes = new Dictionary<string, object?>() },
      new LogEntry { Stream = "test-stream", Timestamp = t3, Level = "info", Message = "c", Attributes = new Dictionary<string, object?>() }
    };

    await ParquetWriter.WriteBatchAsync(entries, outputPath);

    await using var fs = File.OpenRead(outputPath);
    using var reader = await global::Parquet.ParquetReader.CreateAsync(fs);
    using var rg = reader.OpenRowGroupReader(0);
    var tsField = reader.Schema.GetDataFields().Single(f => f.Name == "_t");
    var stats = rg.GetStatistics(tsField);

    stats.Should().NotBeNull();
    var minDt = stats!.MinValue is DateTimeOffset minDto ? minDto.UtcDateTime : (DateTime)stats.MinValue!;
    var maxDt = stats.MaxValue is DateTimeOffset maxDto ? maxDto.UtcDateTime : (DateTime)stats.MaxValue!;
    minDt.Should().Be(t1);
    maxDt.Should().Be(t3);
  }

  private static LogEntry CreateEntry(string message = "test", string level = "info")
  {
    return new LogEntry {
      Stream = "test-stream",
      Timestamp = DateTime.UtcNow,
      Level = level,
      Message = message,
      Attributes = new Dictionary<string, object?>()
    };
  }
}
