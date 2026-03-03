namespace Lumina.Core.Models;

/// <summary>
/// Compaction cursor tracking progress with validation metadata.
/// Used to track the last compacted position for each stream.
/// </summary>
public sealed class CompactionCursor
{
  /// <summary>
  /// Gets the stream name this cursor belongs to.
  /// </summary>
  public required string Stream { get; init; }

  /// <summary>
  /// Gets or sets the last compacted WAL file name.
  /// </summary>
  public string? LastCompactedWalFile { get; set; }

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

  // --- Validation fields for cursor resiliency ---

  /// <summary>
  /// Gets or sets the size in bytes of the last compacted WAL file.
  /// Used for sanity checking during recovery.
  /// </summary>
  public long? LastWalFileSize { get; set; }

  /// <summary>
  /// Gets or sets the number of entries in the last Parquet file.
  /// Used for sanity checking during recovery.
  /// </summary>
  public int? LastParquetEntryCount { get; set; }

  /// <summary>
  /// Gets or sets the checksum of the cursor data (computed before save).
  /// This is a runtime-only field and is not serialized.
  /// </summary>
  [System.Text.Json.Serialization.JsonIgnore]
  public uint? DataChecksum { get; set; }
}
