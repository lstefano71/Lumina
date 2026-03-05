using System.Text.Json.Serialization;

namespace Lumina.Ingestion.Models;

/// <summary>
/// Batch ingestion request for multiple log entries.
/// </summary>
public sealed class BatchLogIngestRequest
{
  /// <summary>
  /// Gets or sets the default stream name for all entries in this batch.
  /// Can be overridden by individual entries.
  /// </summary>
  [JsonPropertyName("_s")]
  public required string Stream { get; init; }

  /// <summary>
  /// Gets or sets the log entries in this batch.
  /// </summary>
  [JsonPropertyName("entries")]
  public required IReadOnlyList<LogIngestRequest> Entries { get; init; }
}