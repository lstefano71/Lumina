namespace Lumina.Core.Models;

/// <summary>
/// Represents a normalized log entry in the unified internal format.
/// This is the core domain model used throughout the system.
/// </summary>
public sealed class LogEntry
{
  /// <summary>
  /// Gets the stream name for this log entry.
  /// Streams are logical partitions for organizing log data.
  /// </summary>
  public required string Stream { get; init; }

  /// <summary>
  /// Gets the timestamp when the log entry was created.
  /// </summary>
  public required DateTime Timestamp { get; init; }

  /// <summary>
  /// Gets the log level (e.g., "debug", "info", "warn", "error", "fatal").
  /// </summary>
  public required string Level { get; init; }

  /// <summary>
  /// Gets the log message content.
  /// </summary>
  public required string Message { get; init; }

  /// <summary>
  /// Gets the attribute dictionary containing additional structured data.
  /// Keys are attribute names, values can be any JSON-serializable type.
  /// </summary>
  public Dictionary<string, object?> Attributes { get; init; } = new();

  /// <summary>
  /// Gets the trace ID for distributed tracing correlation (optional).
  /// </summary>
  public string? TraceId { get; init; }

  /// <summary>
  /// Gets the span ID for distributed tracing correlation (optional).
  /// </summary>
  public string? SpanId { get; init; }

  /// <summary>
  /// Gets the duration in milliseconds for timed operations (optional).
  /// </summary>
  public int? DurationMs { get; init; }

  /// <summary>
  /// Gets or sets the WAL offset for this entry (set during read).
  /// </summary>
  public long Offset { get; set; }
}
