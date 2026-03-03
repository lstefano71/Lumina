namespace Lumina.Core.Models;

/// <summary>
/// Compaction cursor tracking progress.
/// Used to track the last compacted position for each stream.
/// </summary>
public sealed class CompactionCursor
{
  /// <summary>
  /// Gets the stream name this cursor belongs to.
  /// </summary>
  public required string Stream { get; init; }

  /// <summary>
  /// Gets or sets the last compacted offset in the WAL file.
  /// </summary>
  public long LastCompactedOffset { get; set; }

  /// <summary>
  /// Gets or sets the timestamp of the last compaction.
  /// </summary>
  public DateTime LastCompactionTime { get; set; }

  /// <summary>
  /// Gets or sets the last Parquet file produced by compaction.
  /// </summary>
  public string? LastParquetFile { get; set; }
}