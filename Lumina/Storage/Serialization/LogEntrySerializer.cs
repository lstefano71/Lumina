using Lumina.Core.Models;

using MessagePack;

namespace Lumina.Storage.Serialization;

/// <summary>
/// High-performance MessagePack serializer for log entries.
/// </summary>
public static class LogEntrySerializer
{
  private static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
      .WithCompression(MessagePackCompression.Lz4BlockArray);

  /// <summary>
  /// Serializes a single log entry to MessagePack format.
  /// </summary>
  /// <param name="entry">The log entry to serialize.</param>
  /// <returns>The serialized byte array.</returns>
  public static byte[] Serialize(LogEntry entry)
  {
    var serializableEntry = new SerializableLogEntry(entry);
    return MessagePackSerializer.Serialize(serializableEntry, Options);
  }

  /// <summary>
  /// Serializes a batch of log entries efficiently.
  /// </summary>
  /// <param name="entries">The log entries to serialize.</param>
  /// <returns>The serialized byte array.</returns>
  public static byte[] SerializeBatch(IReadOnlyList<LogEntry> entries)
  {
    var serializableEntries = new SerializableLogEntry[entries.Count];
    for (int i = 0; i < entries.Count; i++) {
      serializableEntries[i] = new SerializableLogEntry(entries[i]);
    }
    return MessagePackSerializer.Serialize(serializableEntries, Options);
  }

  /// <summary>
  /// Serializes a log entry to a pre-allocated buffer.
  /// </summary>
  /// <param name="entry">The log entry to serialize.</param>
  /// <param name="buffer">The destination buffer.</param>
  /// <returns>The number of bytes written.</returns>
  public static int Serialize(LogEntry entry, Span<byte> buffer)
  {
    var serializableEntry = new SerializableLogEntry(entry);
    var bytes = MessagePackSerializer.Serialize(serializableEntry, Options);
    bytes.CopyTo(buffer);
    return bytes.Length;
  }

  /// <summary>
  /// Gets the serialized size of a log entry without actually serializing.
  /// </summary>
  /// <param name="entry">The log entry to measure.</param>
  /// <returns>The size in bytes.</returns>
  public static int GetSerializedSize(LogEntry entry)
  {
    var serializableEntry = new SerializableLogEntry(entry);
    return MessagePackSerializer.Serialize(serializableEntry, Options).Length;
  }
}

/// <summary>
/// MessagePack-serializable representation of a log entry.
/// Uses integers for timestamp to optimize serialization performance.
/// </summary>
[MessagePackObject]
public sealed class SerializableLogEntry
{
  /// <summary>
  /// Gets or sets the stream name.
  /// </summary>
  [Key(0)]
  public string Stream { get; set; } = string.Empty;

  /// <summary>
  /// Gets or sets the timestamp as ticks for efficient serialization.
  /// </summary>
  [Key(1)]
  public long TimestampTicks { get; set; }

  /// <summary>
  /// Gets or sets the log level.
  /// </summary>
  [Key(2)]
  public string Level { get; set; } = string.Empty;

  /// <summary>
  /// Gets or sets the message content.
  /// </summary>
  [Key(3)]
  public string Message { get; set; } = string.Empty;

  /// <summary>
  /// Gets or sets the attributes dictionary.
  /// </summary>
  [Key(4)]
  public Dictionary<string, object?>? Attributes { get; set; }

  /// <summary>
  /// Gets or sets the trace ID.
  /// </summary>
  [Key(5)]
  public string? TraceId { get; set; }

  /// <summary>
  /// Gets or sets the span ID.
  /// </summary>
  [Key(6)]
  public string? SpanId { get; set; }

  /// <summary>
  /// Gets or sets the duration in milliseconds.
  /// </summary>
  [Key(7)]
  public int? DurationMs { get; set; }

  /// <summary>
  /// Initializes a new instance of the SerializableLogEntry class.
  /// </summary>
  public SerializableLogEntry() { }

  /// <summary>
  /// Initializes a new instance from a LogEntry.
  /// </summary>
  /// <param name="entry">The source log entry.</param>
  public SerializableLogEntry(LogEntry entry)
  {
    Stream = entry.Stream;
    TimestampTicks = entry.Timestamp.Ticks;
    Level = entry.Level;
    Message = entry.Message;
    Attributes = entry.Attributes.Count > 0 ? entry.Attributes : null;
    TraceId = entry.TraceId;
    SpanId = entry.SpanId;
    DurationMs = entry.DurationMs;
  }

  /// <summary>
  /// Converts to a LogEntry.
  /// </summary>
  /// <returns>The LogEntry instance.</returns>
  public LogEntry ToLogEntry()
  {
    return new LogEntry {
      Stream = Stream,
      Timestamp = new DateTime(TimestampTicks, DateTimeKind.Utc),
      Level = Level,
      Message = Message,
      Attributes = Attributes ?? new Dictionary<string, object?>(),
      TraceId = TraceId,
      SpanId = SpanId,
      DurationMs = DurationMs
    };
  }
}