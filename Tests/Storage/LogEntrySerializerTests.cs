using FluentAssertions;

using Lumina.Core.Models;
using Lumina.Storage.Serialization;

using Xunit;

namespace Lumina.Tests.Storage;

public class LogEntrySerializerTests
{
  [Fact]
  public void Serialize_ShouldReturnBytes()
  {
    // Arrange
    var entry = CreateTestEntry();

    // Act
    var bytes = LogEntrySerializer.Serialize(entry);

    // Assert
    bytes.Should().NotBeEmpty();
  }

  [Fact]
  public void Deserialize_ShouldReconstructEntry()
  {
    // Arrange
    var entry = CreateTestEntry();
    var bytes = LogEntrySerializer.Serialize(entry);

    // Act
    var deserialized = LogEntryDeserializer.Deserialize(bytes);

    // Assert
    deserialized.Stream.Should().Be(entry.Stream);
    deserialized.Timestamp.Should().Be(entry.Timestamp);
    deserialized.Level.Should().Be(entry.Level);
    deserialized.Message.Should().Be(entry.Message);
  }

  [Fact]
  public void SerializeBatch_ShouldSerializeMultipleEntries()
  {
    // Arrange
    var entries = new[]
    {
            CreateTestEntry(message: "Message 1"),
            CreateTestEntry(message: "Message 2"),
            CreateTestEntry(message: "Message 3")
        };

    // Act
    var bytes = LogEntrySerializer.SerializeBatch(entries);

    // Assert
    bytes.Should().NotBeEmpty();
  }

  [Fact]
  public void DeserializeBatch_ShouldReconstructAllEntries()
  {
    // Arrange
    var entries = new[]
    {
            CreateTestEntry(message: "Message 1"),
            CreateTestEntry(message: "Message 2"),
            CreateTestEntry(message: "Message 3")
        };
    var bytes = LogEntrySerializer.SerializeBatch(entries);

    // Act
    var deserialized = LogEntryDeserializer.DeserializeBatch(bytes);

    // Assert
    deserialized.Should().HaveCount(3);
    deserialized[0].Message.Should().Be("Message 1");
    deserialized[1].Message.Should().Be("Message 2");
    deserialized[2].Message.Should().Be("Message 3");
  }

  [Fact]
  public void Serialize_ShouldHandleNullAttributes()
  {
    // Arrange
    var entry = new LogEntry {
      Stream = "test-stream",
      Timestamp = DateTime.UtcNow,
      Level = "info",
      Message = "Test message",
      Attributes = new Dictionary<string, object?>()
    };

    // Act
    var bytes = LogEntrySerializer.Serialize(entry);
    var deserialized = LogEntryDeserializer.Deserialize(bytes);

    // Assert
    deserialized.Attributes.Should().NotBeNull();
    deserialized.Attributes.Should().BeEmpty();
  }

  [Fact]
  public void Serialize_ShouldHandleComplexAttributes()
  {
    // Arrange
    var entry = new LogEntry {
      Stream = "test-stream",
      Timestamp = DateTime.UtcNow,
      Level = "error",
      Message = "Complex test message",
      Attributes = new Dictionary<string, object?> {
        ["string_value"] = "hello",
        ["int_value"] = 42,
        ["bool_value"] = true,
        ["double_value"] = 3.14159,
        ["null_value"] = null
      },
      TraceId = "trace-abc-123",
      SpanId = "span-xyz-789",
      DurationMs = 500
    };

    // Act
    var bytes = LogEntrySerializer.Serialize(entry);
    var deserialized = LogEntryDeserializer.Deserialize(bytes);

    // Assert
    deserialized.Attributes["string_value"].Should().Be("hello");
    deserialized.Attributes["int_value"].Should().Be(42);
    deserialized.Attributes["bool_value"].Should().Be(true);
    deserialized.Attributes["double_value"].Should().Be(3.14159);
    deserialized.Attributes["null_value"].Should().BeNull();
    deserialized.TraceId.Should().Be("trace-abc-123");
    deserialized.SpanId.Should().Be("span-xyz-789");
    deserialized.DurationMs.Should().Be(500);
  }

  [Fact]
  public void TryDeserialize_ShouldReturnTrueForValidData()
  {
    // Arrange
    var entry = CreateTestEntry();
    var bytes = LogEntrySerializer.Serialize(entry);

    // Act
    var result = LogEntryDeserializer.TryDeserialize(bytes, out var deserialized);

    // Assert
    result.Should().BeTrue();
    deserialized.Should().NotBeNull();
    deserialized!.Message.Should().Be(entry.Message);
  }

  [Fact]
  public void TryDeserialize_ShouldReturnFalseForInvalidData()
  {
    // Arrange
    var invalidBytes = new byte[] { 0x01, 0x02, 0x03, 0x04 };

    // Act
    var result = LogEntryDeserializer.TryDeserialize(invalidBytes, out var deserialized);

    // Assert
    result.Should().BeFalse();
    deserialized.Should().BeNull();
  }

  [Fact]
  public void GetSerializedSize_ShouldReturnPositiveValue()
  {
    // Arrange
    var entry = CreateTestEntry();

    // Act
    var size = LogEntrySerializer.GetSerializedSize(entry);

    // Assert
    size.Should().BeGreaterThan(0);
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