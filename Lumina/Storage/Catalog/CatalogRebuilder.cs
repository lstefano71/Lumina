using Lumina.Storage.Parquet;

namespace Lumina.Storage.Catalog;

/// <summary>
/// Rebuilds the catalog from disk when the catalog is missing or corrupted.
/// Uses Parquet statistics extraction for accurate time ranges.
/// </summary>
public sealed class CatalogRebuilder
{
  private readonly ILogger<CatalogRebuilder> _logger;

  public CatalogRebuilder(ILogger<CatalogRebuilder> logger)
  {
    _logger = logger ?? throw new ArgumentNullException(nameof(logger));
  }

  /// <summary>
  /// Rebuilds the catalog by scanning L1 and L2 directories.
  /// </summary>
  /// <param name="l1Directory">The L1 Parquet directory.</param>
  /// <param name="l2Directory">The L2 Parquet directory.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The rebuilt catalog.</returns>
  public async Task<StreamCatalog> RecoverFromDiskAsync(
      string l1Directory,
      string l2Directory,
      CancellationToken cancellationToken = default)
  {
    _logger.LogInformation("Starting catalog recovery from disk: L1={L1}, L2={L2}", l1Directory, l2Directory);

    var entries = new List<CatalogEntry>();

    // Scan L1 files
    if (Directory.Exists(l1Directory)) {
      var l1Entries = await ScanL1FilesAsync(l1Directory, cancellationToken);
      entries.AddRange(l1Entries);
      _logger.LogInformation("Found {Count} L1 files during recovery", l1Entries.Count);
    }

    // Scan L2 files
    if (Directory.Exists(l2Directory)) {
      var l2Entries = await ScanL2FilesAsync(l2Directory, cancellationToken);
      entries.AddRange(l2Entries);
      _logger.LogInformation("Found {Count} L2 files during recovery", l2Entries.Count);
    }

    // Resolve conflicts with L2 priority
    var resolvedCatalog = ResolveConflicts(entries);

    _logger.LogInformation(
        "Catalog recovery complete: {Count} entries after conflict resolution",
        resolvedCatalog.Entries.Count);

    return resolvedCatalog;
  }

  /// <summary>
  /// Scans L1 directory for Parquet files.
  /// </summary>
  /// <param name="l1Directory">The L1 directory path.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>List of catalog entries from L1 files.</returns>
  public async Task<IReadOnlyList<CatalogEntry>> ScanL1FilesAsync(
      string l1Directory,
      CancellationToken cancellationToken = default)
  {
    var entries = new List<CatalogEntry>();

    if (!Directory.Exists(l1Directory)) {
      return entries;
    }

    var files = Directory.GetFiles(l1Directory, "*.parquet", SearchOption.AllDirectories);

    foreach (var file in files) {
      cancellationToken.ThrowIfCancellationRequested();

      var entry = await CreateEntryFromFileAsync(file, StorageLevel.L1, cancellationToken);
      if (entry != null) {
        entries.Add(entry);
      }
    }

    return entries;
  }

  /// <summary>
  /// Scans L2 directory for Parquet files.
  /// </summary>
  /// <param name="l2Directory">The L2 directory path.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>List of catalog entries from L2 files.</returns>
  public async Task<IReadOnlyList<CatalogEntry>> ScanL2FilesAsync(
      string l2Directory,
      CancellationToken cancellationToken = default)
  {
    var entries = new List<CatalogEntry>();

    if (!Directory.Exists(l2Directory)) {
      return entries;
    }

    var files = Directory.GetFiles(l2Directory, "*.parquet", SearchOption.AllDirectories);

    foreach (var file in files) {
      cancellationToken.ThrowIfCancellationRequested();

      var entry = await CreateEntryFromFileAsync(file, StorageLevel.L2, cancellationToken);
      if (entry != null) {
        entries.Add(entry);
      }
    }

    return entries;
  }

  /// <summary>
  /// Resolves conflicts where L1 and L2 files have overlapping time ranges.
  /// L2 files take priority (they are consolidated and more efficient).
  /// </summary>
  /// <param name="entries">All entries found during scan.</param>
  /// <returns>Resolved catalog with no duplicates.</returns>
  public StreamCatalog ResolveConflicts(IEnumerable<CatalogEntry> entries)
  {
    var entryList = entries.ToList();

    // Group by stream name
    var streamGroups = entryList
        .GroupBy(e => e.StreamName)
        .ToList();

    var resolvedEntries = new List<CatalogEntry>();

    foreach (var streamGroup in streamGroups) {
      var streamEntries = streamGroup.ToList();
      var l2Entries = streamEntries.Where(e => e.Level == StorageLevel.L2).ToList();
      var l1Entries = streamEntries.Where(e => e.Level == StorageLevel.L1).ToList();

      // Collect time ranges covered by L2 files
      var l2TimeRanges = l2Entries.Select(e => (e.MinTime, e.MaxTime)).ToList();

      // Add all L2 entries
      resolvedEntries.AddRange(l2Entries);

      // Add L1 entries that don't overlap with any L2 entry
      foreach (var l1Entry in l1Entries) {
        bool overlapsWithL2 = l2TimeRanges.Any(l2Range =>
            l1Entry.MinTime <= l2Range.MaxTime && l1Entry.MaxTime >= l2Range.MinTime);

        if (!overlapsWithL2) {
          resolvedEntries.Add(l1Entry);
        } else {
          _logger.LogDebug(
              "Conflict resolved for {Path}: L1 time range [{MinTime}, {MaxTime}] overlaps with L2 file, skipping",
              l1Entry.FilePath, l1Entry.MinTime, l1Entry.MaxTime);
        }
      }
    }

    return new StreamCatalog {
      Entries = resolvedEntries,
      LastModified = DateTime.UtcNow,
      Version = 1
    };
  }

  /// <summary>
  /// Creates a catalog entry from a Parquet file using statistics extraction.
  /// </summary>
  /// <param name="filePath">The file path.</param>
  /// <param name="level">The storage level.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The catalog entry, or null if file info couldn't be extracted.</returns>
  private async Task<CatalogEntry?> CreateEntryFromFileAsync(string filePath, StorageLevel level, CancellationToken cancellationToken)
  {
    var fileInfo = new FileInfo(filePath);
    if (!fileInfo.Exists) {
      return null;
    }

    // Extract time bounds from Parquet file
    var timeBounds = await ParquetStatisticsReader.ExtractTimeBoundsAsync(filePath, cancellationToken);
    if (timeBounds == null) {
      _logger.LogWarning("Could not extract time bounds from {Path}, skipping", filePath);
      return null;
    }

    var (minTime, maxTime) = timeBounds.Value;

    // Extract stream name from file path (directory name)
    var streamName = ExtractStreamNameFromPath(filePath);
    if (string.IsNullOrEmpty(streamName)) {
      _logger.LogWarning("Could not extract stream name from {Path}, skipping", filePath);
      return null;
    }

    // Get row count from Parquet file
    long rowCount = await ParquetStatisticsReader.GetRowCountAsync(filePath, cancellationToken);

    return new CatalogEntry {
      StreamName = streamName,
      MinTime = minTime,
      MaxTime = maxTime,
      FilePath = Path.GetFullPath(filePath),
      Level = level,
      RowCount = rowCount,
      FileSizeBytes = fileInfo.Length,
      AddedAt = DateTime.UtcNow,
      CompactionTier = (int)level
    };
  }

  /// <summary>
  /// Extracts the stream name from a file path.
  /// The stream name is the parent directory name.
  /// </summary>
  private static string? ExtractStreamNameFromPath(string filePath)
  {
    var directory = Path.GetDirectoryName(filePath);
    if (string.IsNullOrEmpty(directory)) {
      return null;
    }

    return Path.GetFileName(directory);
  }
}