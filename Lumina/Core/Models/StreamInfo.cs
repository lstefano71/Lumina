namespace Lumina.Core.Models;

/// <summary>
/// Represents a stream identifier and its metadata.
/// Streams are logical partitions for organizing log data.
/// </summary>
public sealed class StreamInfo
{
  /// <summary>
  /// Gets the name of the stream.
  /// </summary>
  public required string Name { get; init; }

  /// <summary>
  /// Gets the creation timestamp of the stream.
  /// </summary>
  public DateTime CreatedAt { get; init; }

  /// <summary>
  /// Gets or sets the total number of entries in this stream.
  /// </summary>
  public long TotalEntries { get; set; }

  /// <summary>
  /// Gets or sets the total size in bytes of all entries in this stream.
  /// </summary>
  public long TotalSizeBytes { get; set; }

  /// <summary>
  /// Gets or sets the timestamp of the last entry written to this stream.
  /// </summary>
  public DateTime? LastEntryAt { get; set; }

  /// <summary>
  /// Gets or sets the retention policy for this stream.
  /// </summary>
  public RetentionPolicy? Retention { get; set; }
}