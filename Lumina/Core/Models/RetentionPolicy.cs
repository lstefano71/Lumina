namespace Lumina.Core.Models;

/// <summary>
/// Retention policy for a stream.
/// Defines how long data is kept before being automatically deleted.
/// </summary>
public sealed class RetentionPolicy
{
  /// <summary>
  /// Gets the time-to-live in days for entries in this stream.
  /// Default is 30 days.
  /// </summary>
  public int TtlDays { get; init; } = 30;

  /// <summary>
  /// Gets the maximum total size in bytes for this stream.
  /// When exceeded, oldest entries are deleted first.
  /// Default is unlimited (long.MaxValue).
  /// </summary>
  public long MaxSizeBytes { get; init; } = long.MaxValue;
}