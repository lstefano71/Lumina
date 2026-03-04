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
  /// Default is "data/storage/parquet".
  /// </summary>
  public string ParquetOutputDirectory { get; init; } = "data/storage/parquet";

  /// <summary>
  /// Gets the L1 output directory for Parquet micro-batch files.
  /// Default is "data/storage/l1".
  /// </summary>
  public string L1Directory { get; init; } = "data/storage/l1";

  /// <summary>
  /// Gets the L2 output directory for daily consolidated Parquet files.
  /// Default is "data/storage/l2".
  /// </summary>
  public string L2Directory { get; init; } = "data/storage/l2";

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
  /// Default is "data/storage/cursors".
  /// </summary>
  public string CursorDirectory { get; init; } = "data/storage/cursors";

  // --- Cursor resiliency settings ---

  /// <summary>
  /// Gets a value indicating whether cursor file validation is enabled.
  /// When enabled, cursor files are validated with checksum verification.
  /// Default is true.
  /// </summary>
  public bool EnableCursorValidation { get; init; } = true;

  /// <summary>
  /// Gets a value indicating whether cursor recovery is enabled.
  /// When enabled, corrupted cursors are automatically recovered from WAL/Parquet files.
  /// Default is true.
  /// </summary>
  public bool EnableCursorRecovery { get; init; } = true;

  /// <summary>
  /// Gets a value indicating whether strict validation mode is enabled.
  /// When enabled, cross-validation with filesystem is performed on cursor load.
  /// Default is false.
  /// </summary>
  public bool StrictValidationMode { get; init; } = false;

  // --- Catalog settings ---

  /// <summary>
  /// Gets the directory where the stream catalog is stored.
  /// Default is "data/storage/catalog".
  /// </summary>
  public string CatalogDirectory { get; init; } = "data/storage/catalog";

  /// <summary>
  /// Gets a value indicating whether to rebuild catalog on startup if corrupted.
  /// Default is true.
  /// </summary>
  public bool EnableCatalogAutoRebuild { get; init; } = true;

  /// <summary>
  /// Gets a value indicating whether to run garbage collection on startup.
  /// Default is true.
  /// </summary>
  public bool EnableCatalogStartupGc { get; init; } = true;
}
