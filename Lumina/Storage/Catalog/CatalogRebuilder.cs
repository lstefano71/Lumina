namespace Lumina.Storage.Catalog;

/// <summary>
/// Rebuilds the catalog from disk when the catalog is missing or corrupted.
/// Implements L2 priority conflict resolution.
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

      var entry = await CreateEntryFromFileAsync(file, StorageLevel.L1);
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

      var entry = await CreateEntryFromFileAsync(file, StorageLevel.L2);
      if (entry != null) {
        entries.Add(entry);
      }
    }

    return entries;
  }

  /// <summary>
  /// Resolves conflicts where both L1 and L2 files exist for the same stream+date.
  /// L2 files take priority (they are consolidated and more efficient).
  /// </summary>
  /// <param name="entries">All entries found during scan.</param>
  /// <returns>Resolved catalog with no duplicates.</returns>
  public StreamCatalog ResolveConflicts(IEnumerable<CatalogEntry> entries)
  {
    var entryList = entries.ToList();

    // Group by (StreamName, Date)
    var groups = entryList
        .GroupBy(e => new { e.StreamName, e.Date.Date })
        .ToList();

    var resolvedEntries = new List<CatalogEntry>();

    foreach (var group in groups) {
      var l2Entries = group.Where(e => e.Level == StorageLevel.L2).ToList();
      var l1Entries = group.Where(e => e.Level == StorageLevel.L1).ToList();

      if (l2Entries.Count > 0) {
        // L2 takes priority - use all L2 files
        resolvedEntries.AddRange(l2Entries);

        if (l1Entries.Count > 0) {
          _logger.LogDebug(
              "Conflict resolved for {Stream}/{Date}: using {L2Count} L2 file(s), ignoring {L1Count} L1 file(s)",
              group.Key.StreamName, group.Key.Date, l2Entries.Count, l1Entries.Count);
        }
      } else {
        // No L2, use L1 files
        resolvedEntries.AddRange(l1Entries);
      }
    }

    return new StreamCatalog {
      Entries = resolvedEntries,
      LastModified = DateTime.UtcNow,
      Version = 1
    };
  }

  /// <summary>
  /// Creates a catalog entry from a Parquet file.
  /// </summary>
  /// <param name="filePath">The file path.</param>
  /// <param name="level">The storage level.</param>
  /// <returns>The catalog entry, or null if file info couldn't be parsed.</returns>
  private async Task<CatalogEntry?> CreateEntryFromFileAsync(string filePath, StorageLevel level)
  {
    var fileInfo = new FileInfo(filePath);
    if (!fileInfo.Exists) {
      return null;
    }

    // Parse stream name and date from file path
    var parsedInfo = ParseFilePath(filePath, level);
    if (parsedInfo == null) {
      _logger.LogWarning("Could not parse file info from {Path}, skipping", filePath);
      return null;
    }

    // Get row count from Parquet file
    long rowCount = await GetRowCountAsync(filePath);

    return new CatalogEntry {
      StreamName = parsedInfo.StreamName,
      Date = parsedInfo.Date,
      FilePath = Path.GetFullPath(filePath),
      Level = level,
      RowCount = rowCount,
      FileSizeBytes = fileInfo.Length,
      AddedAt = DateTime.UtcNow
    };
  }

  /// <summary>
  /// Parses stream name and date from file path.
  /// </summary>
  /// <param name="filePath">The file path.</param>
  /// <param name="level">The storage level.</param>
  /// <returns>Parsed info, or null if parsing failed.</returns>
  private static ParsedFileInfo? ParseFilePath(string filePath, StorageLevel level)
  {
    var fileName = Path.GetFileNameWithoutExtension(filePath);
    var parts = fileName.Split('_');

    if (parts.Length < 2) {
      return null;
    }

    // Extract stream name - could be multiple parts if stream name contains underscores
    // L1 format: stream_starttime_endtime.parquet
    // L2 format: stream_yyyyMMdd_consolidated.parquet

    // For L2, look for "_consolidated" suffix
    if (level == StorageLevel.L2 && fileName.EndsWith("_consolidated")) {
      // Format: stream_yyyyMMdd_consolidated
      // Stream name is everything before the date
      var consolidatedIndex = Array.FindIndex(parts, p => p == "consolidated");
      if (consolidatedIndex >= 2) {
        var streamName = string.Join("_", parts.Take(consolidatedIndex - 1));
        var dateStr = parts[consolidatedIndex - 1];

        if (DateTime.TryParseExact(dateStr, "yyyyMMdd", null,
            System.Globalization.DateTimeStyles.None, out var date)) {
          return new ParsedFileInfo { StreamName = streamName, Date = date.Date };
        }
      }
    } else {
      // L1 format: stream_starttime_endtime
      // Try to find where stream name ends and timestamp begins
      // Timestamps are typically yyyyMMddHHmmss format (14 digits)

      for (int i = 1; i < parts.Length; i++) {
        if (parts[i].Length >= 8 && parts[i].All(char.IsDigit)) {
          var streamName = string.Join("_", parts.Take(i));
          var dateStr = parts[i].Length >= 8
              ? parts[i].Substring(0, 8)
              : parts[i];

          if (DateTime.TryParseExact(dateStr, "yyyyMMdd", null,
              System.Globalization.DateTimeStyles.None, out var date)) {
            return new ParsedFileInfo { StreamName = streamName, Date = date.Date };
          }
        }
      }
    }

    return null;
  }

  /// <summary>
  /// Gets the row count from a Parquet file.
  /// </summary>
  /// <param name="filePath">The file path.</param>
  /// <returns>Row count, or 0 if unable to read.</returns>
  private async Task<long> GetRowCountAsync(string filePath)
  {
    try {
      await using var stream = File.OpenRead(filePath);
      using var reader = await global::Parquet.ParquetReader.CreateAsync(stream);
      return reader.RowGroups.Sum(rg => rg.RowCount);
    } catch (Exception ex) {
      _logger.LogWarning(ex, "Failed to read row count from {Path}", filePath);
      return 0;
    }
  }

  private sealed class ParsedFileInfo
  {
    public required string StreamName { get; init; }
    public required DateTime Date { get; init; }
  }
}