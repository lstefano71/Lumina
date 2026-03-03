namespace Lumina.Core.Configuration;

/// <summary>
/// Configuration settings for the compaction process.
/// </summary>
public sealed class CompactionSettings
{
  /// <summary>
  /// Gets the interval in minutes between compaction runs.
  /// Default is 10 minutes.
  /// </summary>
  public int IntervalMinutes { get; init; } = 10;

  /// <summary>
  /// Gets the interval in minutes for L1 compaction (WAL → Parquet).
  /// Default is 10 minutes.
  /// </summary>
  public int L1IntervalMinutes { get; init; } = 10;

  /// <summary>
  /// Gets the interval in hours for L2 compaction (daily consolidation).
  /// Default is 24 hours.
  /// </summary>
  public int L2IntervalHours { get; init; } = 24;

  /// <summary>
  /// Gets the maximum number of dynamic keys before they overflow into _meta column.
  /// Default is 100.
  /// </summary>
  public int MaxDynamicKeys { get; init; } = 100;

  /// <summary>
  /// Gets the output directory for Parquet files.
  /// Default is "data/parquet".
  /// </summary>
  public string ParquetOutputDirectory { get; init; } = "data/parquet";

  /// <summary>
  /// Gets the L1 output directory for Parquet files.
  /// Default is "data/l1".
  /// </summary>
  public string L1Directory { get; init; } = "data/l1";

  /// <summary>
  /// Gets the L2 output directory for daily consolidated Parquet files.
  /// Default is "data/l2".
  /// </summary>
  public string L2Directory { get; init; } = "data/l2";

  /// <summary>
  /// Gets the L1 window duration for time-based compaction.
  /// Default is 10 minutes.
  /// </summary>
  public TimeSpan L1Window => TimeSpan.FromMinutes(L1IntervalMinutes);

  /// <summary>
  /// Gets the maximum number of entries per Parquet file.
  /// Default is 100,000.
  /// </summary>
  public int MaxEntriesPerFile { get; init; } = 100_000;

  /// <summary>
  /// Gets the cursor directory for storing compaction cursors.
  /// Default is "data/cursors".
  /// </summary>
  public string CursorDirectory { get; init; } = "data/cursors";
}
