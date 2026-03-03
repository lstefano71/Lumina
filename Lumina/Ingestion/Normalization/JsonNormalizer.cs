using System.Text.Json;
using Lumina.Core.Models;
using Lumina.Ingestion.Models;

namespace Lumina.Ingestion.Normalization;

/// <summary>
/// Normalizes JSON ingestion requests to LogEntry objects.
/// </summary>
public static class JsonNormalizer
{
  /// <summary>
  /// Normalizes a single log ingestion request to a LogEntry.
  /// </summary>
  /// <param name="request">The ingestion request.</param>
  /// <returns>The normalized LogEntry.</returns>
  public static LogEntry Normalize(LogIngestRequest request)
  {
    return new LogEntry {
      Stream = request.Stream,
      Timestamp = EnsureUtc(request.Timestamp),
      Level = NormalizeLevel(request.Level),
      Message = request.Message,
      Attributes = UnwrapAttributes(request.Attributes),
      TraceId = request.TraceId,
      SpanId = request.SpanId,
      DurationMs = request.DurationMs
    };
  }

  /// <summary>
  /// Normalizes a batch ingestion request to LogEntry objects.
  /// </summary>
  /// <param name="request">The batch ingestion request.</param>
  /// <returns>The normalized LogEntry objects.</returns>
  public static IReadOnlyList<LogEntry> NormalizeBatch(BatchLogIngestRequest request)
  {
    var entries = new LogEntry[request.Entries.Count];

    for (int i = 0; i < request.Entries.Count; i++) {
      var entry = request.Entries[i];

      // Use batch stream if entry doesn't have a stream
      var stream = string.IsNullOrEmpty(entry.Stream) ? request.Stream : entry.Stream;

      entries[i] = new LogEntry {
        Stream = stream,
        Timestamp = EnsureUtc(entry.Timestamp),
        Level = NormalizeLevel(entry.Level),
        Message = entry.Message,
        Attributes = UnwrapAttributes(entry.Attributes),
        TraceId = entry.TraceId,
        SpanId = entry.SpanId,
        DurationMs = entry.DurationMs
      };
    }

    return entries;
  }

  /// <summary>
  /// Unwraps any <see cref="JsonElement"/> values produced by System.Text.Json
  /// during model binding into plain CLR types that MessagePack can serialize.
  /// </summary>
  private static Dictionary<string, object?> UnwrapAttributes(Dictionary<string, object?>? attributes)
  {
    if (attributes is null || attributes.Count == 0)
      return new Dictionary<string, object?>();

    var result = new Dictionary<string, object?>(attributes.Count);
    foreach (var (key, value) in attributes)
      result[key] = value is JsonElement je ? UnwrapJsonElement(je) : value;
    return result;
  }

  private static object? UnwrapJsonElement(JsonElement element) => element.ValueKind switch {
    JsonValueKind.String  => element.GetString(),
    JsonValueKind.True    => true,
    JsonValueKind.False   => false,
    JsonValueKind.Null    => null,
    JsonValueKind.Number  => element.TryGetInt64(out var l) ? l : element.GetDouble(),
    _                     => element.GetRawText()
  };

  /// <summary>
  /// Normalizes log level to lowercase standard format.
  /// </summary>
  private static string NormalizeLevel(string level)
  {
    var normalized = level.ToLowerInvariant().Trim();

    // Map common variations to standard levels
    return normalized switch {
      "trace" or "tracing" or "verbose" => "trace",
      "debug" or "debugging" => "debug",
      "info" or "information" or "notice" => "info",
      "warn" or "warning" => "warn",
      "error" or "err" or "exception" => "error",
      "fatal" or "critical" or "crit" or "panic" => "fatal",
      _ => normalized
    };
  }

  /// <summary>
  /// Ensures the timestamp is in UTC.
  /// </summary>
  private static DateTime EnsureUtc(DateTime timestamp)
  {
    if (timestamp.Kind == DateTimeKind.Utc) {
      return timestamp;
    }

    if (timestamp.Kind == DateTimeKind.Local) {
      return timestamp.ToUniversalTime();
    }

    // Assume unspecified is UTC
    return DateTime.SpecifyKind(timestamp, DateTimeKind.Utc);
  }
}