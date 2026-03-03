using FluentAssertions;

using Lumina.Ingestion.Models;
using Lumina.Ingestion.Normalization;

using Xunit;

namespace Lumina.Tests.Ingestion;

public class JsonNormalizerTests
{
  // --- Single entry normalization ---

  [Fact]
  public void Normalize_ShouldMapAllFields()
  {
    var request = new LogIngestRequest {
      Stream = "my-stream",
      Timestamp = new DateTime(2025, 6, 15, 12, 0, 0, DateTimeKind.Utc),
      Level = "info",
      Message = "Hello world",
      TraceId = "trace-1",
      SpanId = "span-1",
      DurationMs = 42,
      Attributes = new Dictionary<string, object?> { ["env"] = "prod" }
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Stream.Should().Be("my-stream");
    entry.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
    entry.Level.Should().Be("info");
    entry.Message.Should().Be("Hello world");
    entry.TraceId.Should().Be("trace-1");
    entry.SpanId.Should().Be("span-1");
    entry.DurationMs.Should().Be(42);
    entry.Attributes.Should().ContainKey("env");
  }

  [Fact]
  public void Normalize_NullAttributes_ShouldBeEmptyDictionary()
  {
    var request = new LogIngestRequest {
      Stream = "s",
      Level = "info",
      Message = "m",
      Attributes = null
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Attributes.Should().NotBeNull();
    entry.Attributes.Should().BeEmpty();
  }

  // --- Level normalization ---

  [Theory]
  [InlineData("trace", "trace")]
  [InlineData("TRACE", "trace")]
  [InlineData("verbose", "trace")]
  [InlineData("tracing", "trace")]
  [InlineData("debug", "debug")]
  [InlineData("DEBUG", "debug")]
  [InlineData("debugging", "debug")]
  [InlineData("info", "info")]
  [InlineData("INFO", "info")]
  [InlineData("information", "info")]
  [InlineData("notice", "info")]
  [InlineData("warn", "warn")]
  [InlineData("WARNING", "warn")]
  [InlineData("error", "error")]
  [InlineData("ERR", "error")]
  [InlineData("exception", "error")]
  [InlineData("fatal", "fatal")]
  [InlineData("FATAL", "fatal")]
  [InlineData("critical", "fatal")]
  [InlineData("crit", "fatal")]
  [InlineData("panic", "fatal")]
  public void Normalize_ShouldNormalizeLevelCorrectly(string input, string expected)
  {
    var request = new LogIngestRequest {
      Stream = "s",
      Level = input,
      Message = "m"
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Level.Should().Be(expected);
  }

  [Fact]
  public void Normalize_UnknownLevel_ShouldPassThroughLowercased()
  {
    var request = new LogIngestRequest {
      Stream = "s",
      Level = "CUSTOM_LEVEL",
      Message = "m"
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Level.Should().Be("custom_level");
  }

  // --- Timestamp normalization ---

  [Fact]
  public void Normalize_UtcTimestamp_ShouldStayUtc()
  {
    var utcTime = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    var request = new LogIngestRequest {
      Stream = "s",
      Level = "info",
      Message = "m",
      Timestamp = utcTime
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
    entry.Timestamp.Should().Be(utcTime);
  }

  [Fact]
  public void Normalize_LocalTimestamp_ShouldConvertToUtc()
  {
    var localTime = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local);
    var request = new LogIngestRequest {
      Stream = "s",
      Level = "info",
      Message = "m",
      Timestamp = localTime
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
  }

  [Fact]
  public void Normalize_UnspecifiedTimestamp_ShouldTreatAsUtc()
  {
    var unspecified = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);
    var request = new LogIngestRequest {
      Stream = "s",
      Level = "info",
      Message = "m",
      Timestamp = unspecified
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Timestamp.Kind.Should().Be(DateTimeKind.Utc);
    entry.Timestamp.Hour.Should().Be(12);
  }

  // --- Batch normalization ---

  [Fact]
  public void NormalizeBatch_ShouldNormalizeAllEntries()
  {
    var batch = new BatchLogIngestRequest {
      Stream = "batch-stream",
      Entries = new[]
        {
                new LogIngestRequest { Stream = "", Level = "info", Message = "m1" },
                new LogIngestRequest { Stream = "", Level = "error", Message = "m2" },
                new LogIngestRequest { Stream = "", Level = "debug", Message = "m3" }
            }
    };

    var entries = JsonNormalizer.NormalizeBatch(batch);

    entries.Should().HaveCount(3);
  }

  [Fact]
  public void NormalizeBatch_EmptyEntryStream_ShouldUseBatchStream()
  {
    var batch = new BatchLogIngestRequest {
      Stream = "batch-stream",
      Entries = new[]
        {
                new LogIngestRequest { Stream = "", Level = "info", Message = "m1" }
            }
    };

    var entries = JsonNormalizer.NormalizeBatch(batch);

    entries[0].Stream.Should().Be("batch-stream");
  }

  [Fact]
  public void NormalizeBatch_EntryWithOwnStream_ShouldOverrideBatchStream()
  {
    var batch = new BatchLogIngestRequest {
      Stream = "batch-stream",
      Entries = new[]
        {
                new LogIngestRequest { Stream = "custom-stream", Level = "info", Message = "m1" }
            }
    };

    var entries = JsonNormalizer.NormalizeBatch(batch);

    entries[0].Stream.Should().Be("custom-stream");
  }

  [Fact]
  public void NormalizeBatch_ShouldPreserveAttributes()
  {
    var batch = new BatchLogIngestRequest {
      Stream = "s",
      Entries = new[]
        {
                new LogIngestRequest
                {
                    Stream = "s",
                    Level = "info",
                    Message = "m",
                    Attributes = new Dictionary<string, object?> { ["key"] = "value" }
                }
            }
    };

    var entries = JsonNormalizer.NormalizeBatch(batch);

    entries[0].Attributes.Should().ContainKey("key");
    entries[0].Attributes["key"].Should().Be("value");
  }

  [Fact]
  public void NormalizeBatch_ShouldNormalizeLevels()
  {
    var batch = new BatchLogIngestRequest {
      Stream = "s",
      Entries = new[]
        {
                new LogIngestRequest { Stream = "s", Level = "WARNING", Message = "m1" },
                new LogIngestRequest { Stream = "s", Level = "ERR", Message = "m2" }
            }
    };

    var entries = JsonNormalizer.NormalizeBatch(batch);

    entries[0].Level.Should().Be("warn");
    entries[1].Level.Should().Be("error");
  }

  // --- Edge cases ---

  [Fact]
  public void Normalize_LevelWithWhitespace_ShouldTrim()
  {
    var request = new LogIngestRequest {
      Stream = "s",
      Level = "  info  ",
      Message = "m"
    };

    var entry = JsonNormalizer.Normalize(request);

    entry.Level.Should().Be("info");
  }
}
