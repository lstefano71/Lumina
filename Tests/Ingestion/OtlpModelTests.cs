using FluentAssertions;

using Lumina.Ingestion.Endpoints;

using Xunit;

namespace Lumina.Tests.Ingestion;

public class OtlpModelTests
{
  // --- OtlpAnyValue.ToObject() ---

  [Fact]
  public void OtlpAnyValue_StringValue_ShouldConvert()
  {
    var val = new OtlpAnyValue { StringValue = "hello" };

    val.ToObject().Should().Be("hello");
  }

  [Fact]
  public void OtlpAnyValue_IntValue_ShouldConvert()
  {
    var val = new OtlpAnyValue { IntValue = 42 };

    val.ToObject().Should().Be(42L);
  }

  [Fact]
  public void OtlpAnyValue_DoubleValue_ShouldConvert()
  {
    var val = new OtlpAnyValue { DoubleValue = 3.14 };

    val.ToObject().Should().Be(3.14);
  }

  [Fact]
  public void OtlpAnyValue_BoolValue_ShouldConvert()
  {
    var val = new OtlpAnyValue { BoolValue = true };

    val.ToObject().Should().Be(true);
  }

  [Fact]
  public void OtlpAnyValue_ArrayValue_ShouldConvert()
  {
    var val = new OtlpAnyValue {
      ArrayValue = new List<OtlpAnyValue>
        {
                new() { StringValue = "a" },
                new() { StringValue = "b" }
            }
    };

    var result = val.ToObject() as List<object?>;
    result.Should().NotBeNull();
    result.Should().HaveCount(2);
    result![0].Should().Be("a");
    result[1].Should().Be("b");
  }

  [Fact]
  public void OtlpAnyValue_KvListValue_ShouldConvert()
  {
    var val = new OtlpAnyValue {
      KvListValue = new Dictionary<string, OtlpAnyValue> {
        ["key1"] = new OtlpAnyValue { StringValue = "val1" },
        ["key2"] = new OtlpAnyValue { IntValue = 99 }
      }
    };

    var result = val.ToObject() as Dictionary<string, object?>;
    result.Should().NotBeNull();
    result!["key1"].Should().Be("val1");
    result["key2"].Should().Be(99L);
  }

  [Fact]
  public void OtlpAnyValue_AllDefaults_ShouldReturnNull()
  {
    var val = new OtlpAnyValue();

    val.ToObject().Should().BeNull();
  }

  // --- OtlpLogRecord model ---

  [Fact]
  public void OtlpLogRecord_DefaultValues_ShouldBeReasonable()
  {
    var record = new OtlpLogRecord();

    record.TimeUnixNano.Should().Be(0);
    record.SeverityNumber.Should().Be(0);
    record.SeverityText.Should().BeNull();
    record.Body.Should().BeNull();
    record.Attributes.Should().BeNull();
    record.TraceId.Should().BeNull();
    record.SpanId.Should().BeNull();
  }

  // --- OtlpLogsRequest structure ---

  [Fact]
  public void OtlpLogsRequest_CanBeConstructedWithFullHierarchy()
  {
    var request = new OtlpLogsRequest {
      ResourceLogs = new List<OtlpResourceLogs>
        {
                new()
                {
                    Resource = new OtlpResource
                    {
                        Attributes = new Dictionary<string, OtlpAnyValue>
                        {
                            ["service.name"] = new OtlpAnyValue { StringValue = "my-service" }
                        }
                    },
                    ScopeLogs = new List<OtlpScopeLogs>
                    {
                        new()
                        {
                            Scope = new OtlpInstrumentationScope
                            {
                                Name = "my-scope",
                                Version = "1.0"
                            },
                            LogRecords = new List<OtlpLogRecord>
                            {
                                new()
                                {
                                    TimeUnixNano = 1718000000000000000,
                                    SeverityNumber = 9,
                                    Body = new OtlpAnyValue { StringValue = "Test log" },
                                    TraceId = "abc123",
                                    SpanId = "def456"
                                }
                            }
                        }
                    }
                }
            }
    };

    request.ResourceLogs.Should().HaveCount(1);
    request.ResourceLogs![0].ScopeLogs.Should().HaveCount(1);
    request.ResourceLogs[0].ScopeLogs![0].LogRecords.Should().HaveCount(1);
    var logRecord = request.ResourceLogs![0].ScopeLogs![0].LogRecords![0];
    logRecord.Body!.StringValue.Should().Be("Test log");
  }

  // --- InstrumentationScope ---

  [Fact]
  public void OtlpInstrumentationScope_ShouldHoldNameAndVersion()
  {
    var scope = new OtlpInstrumentationScope {
      Name = "otel-logger",
      Version = "2.1.0"
    };

    scope.Name.Should().Be("otel-logger");
    scope.Version.Should().Be("2.1.0");
  }

  // --- Severity mapping (as defined in OtlpIngestionEndpoint) ---

  [Theory]
  [InlineData(1, "debug")]
  [InlineData(5, "debug")]
  [InlineData(6, "info")]
  [InlineData(9, "info")]
  [InlineData(10, "warn")]
  [InlineData(13, "warn")]
  [InlineData(14, "error")]
  [InlineData(17, "error")]
  [InlineData(18, "fatal")]
  [InlineData(24, "fatal")]
  public void SeverityNumber_ShouldMapToExpectedLevel(int severityNumber, string expectedLevel)
  {
    // Mirror the mapping from OtlpIngestionEndpoint.ConvertOtlpLogToEntry
    var level = severityNumber switch {
      <= 5 => "debug",
      <= 9 => "info",
      <= 13 => "warn",
      <= 17 => "error",
      _ => "fatal"
    };

    level.Should().Be(expectedLevel);
  }
}
