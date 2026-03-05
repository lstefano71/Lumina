using System.Text.Json.Serialization;

namespace Lumina.Ingestion.Models;

/// <summary>
/// JSON ingestion request for a single log entry.
/// System fields use underscore-prefixed JSON names (_s, _t, _l, _m, _traceid, _spanid, _duration_ms).
/// </summary>
public sealed class LogIngestRequest
{
  /// <summary>
  /// Gets or sets the stream name for this log entry.
  /// </summary>
  [JsonPropertyName("_s")]
  public required string Stream { get; init; }

  /// <summary>
  /// Gets or sets the timestamp when the log entry was created.
  /// Defaults to UTC now if not specified.
  /// </summary>
  [JsonPropertyName("_t")]
  public DateTime Timestamp { get; init; } = DateTime.UtcNow;

  /// <summary>
  /// Gets or sets the log level (e.g., "debug", "info", "warn", "error", "fatal"). Optional.
  /// </summary>
  [JsonPropertyName("_l")]
  public string? Level { get; init; }

  /// <summary>
  /// Gets or sets the log message content.
  /// </summary>
  [JsonPropertyName("_m")]
  public required string Message { get; init; }

  /// <summary>
  /// Gets or sets the attribute dictionary containing additional structured data.
  /// </summary>
  public Dictionary<string, object?>? Attributes { get; init; }

  /// <summary>
  /// Gets or sets the trace ID for distributed tracing correlation.
  /// </summary>
  [JsonPropertyName("_traceid")]
  public string? TraceId { get; init; }

  /// <summary>
  /// Gets or sets the span ID for distributed tracing correlation.
  /// </summary>
  [JsonPropertyName("_spanid")]
  public string? SpanId { get; init; }

  /// <summary>
  /// Gets or sets the duration in milliseconds for timed operations.
  /// </summary>
  [JsonPropertyName("_duration_ms")]
  public int? DurationMs { get; init; }
}