using Lumina.Core.Models;

using MessagePack;

using System.Buffers;

namespace Lumina.Storage.Serialization;

/// <summary>
/// High-performance MessagePack deserializer for log entries.
/// </summary>
public static class LogEntryDeserializer
{
  private static readonly MessagePackSerializerOptions Options = MessagePackSerializerOptions.Standard
      .WithCompression(MessagePackCompression.Lz4BlockArray);

  /// <summary>
  /// Deserializes a log entry from MessagePack format.
  /// </summary>
  /// <param name="data">The serialized data.</param>
  /// <returns>The deserialized log entry.</returns>
  public static LogEntry Deserialize(ReadOnlySpan<byte> data)
  {
    var sequence = new ReadOnlySequence<byte>(data.ToArray());
    var serializableEntry = MessagePackSerializer.Deserialize<SerializableLogEntry>(in sequence, Options);
    return serializableEntry.ToLogEntry();
  }

  /// <summary>
  /// Deserializes a batch of log entries.
  /// </summary>
  /// <param name="data">The serialized batch data.</param>
  /// <returns>The deserialized log entries.</returns>
  public static IReadOnlyList<LogEntry> DeserializeBatch(ReadOnlySpan<byte> data)
  {
    var sequence = new ReadOnlySequence<byte>(data.ToArray());
    var serializableEntries = MessagePackSerializer.Deserialize<SerializableLogEntry[]>(in sequence, Options);
    var entries = new LogEntry[serializableEntries.Length];

    for (int i = 0; i < serializableEntries.Length; i++) {
      entries[i] = serializableEntries[i].ToLogEntry();
    }

    return entries;
  }

  /// <summary>
  /// Attempts to deserialize a log entry from MessagePack format.
  /// </summary>
  /// <param name="data">The serialized data.</param>
  /// <param name="entry">The deserialized log entry if successful.</param>
  /// <returns>True if deserialization was successful; otherwise false.</returns>
  public static bool TryDeserialize(ReadOnlySpan<byte> data, out LogEntry? entry)
  {
    try {
      entry = Deserialize(data);
      return true;
    } catch (MessagePackSerializationException) {
      entry = null;
      return false;
    }
  }

  /// <summary>
  /// Deserializes a log entry from a byte array.
  /// </summary>
  /// <param name="data">The serialized data.</param>
  /// <returns>The deserialized log entry.</returns>
  public static LogEntry Deserialize(byte[] data)
  {
    var sequence = new ReadOnlySequence<byte>(data);
    var serializableEntry = MessagePackSerializer.Deserialize<SerializableLogEntry>(in sequence, Options);
    return serializableEntry.ToLogEntry();
  }
}
