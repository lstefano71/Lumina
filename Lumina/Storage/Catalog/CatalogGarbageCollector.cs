namespace Lumina.Storage.Catalog;

/// <summary>
/// Cleans up orphaned files on startup.
/// Orphaned files are Parquet files that exist on disk but are not in the catalog.
/// </summary>
public sealed class CatalogGarbageCollector
{
  private readonly ILogger<CatalogGarbageCollector> _logger;

  public CatalogGarbageCollector(ILogger<CatalogGarbageCollector> logger)
  {
    _logger = logger ?? throw new ArgumentNullException(nameof(logger));
  }

  /// <summary>
  /// Runs garbage collection to remove orphaned files.
  /// </summary>
  /// <param name="catalog">The current catalog state.</param>
  /// <param name="l1Directory">The L1 Parquet directory.</param>
  /// <param name="l2Directory">The L2 Parquet directory.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Number of orphaned files deleted.</returns>
  public async Task<int> RunGcAsync(
      StreamCatalog catalog,
      string l1Directory,
      string l2Directory,
      CancellationToken cancellationToken = default)
  {
    _logger.LogInformation("Starting catalog garbage collection");

    var catalogFilePaths = catalog.Entries
        .Select(e => Path.GetFullPath(e.FilePath))
        .ToHashSet(StringComparer.OrdinalIgnoreCase);

    var deletedCount = 0;

    // Clean L1 directory
    if (Directory.Exists(l1Directory)) {
      deletedCount += await CleanupDirectoryAsync(
          l1Directory, catalogFilePaths, cancellationToken);
    }

    // Clean L2 directory
    if (Directory.Exists(l2Directory)) {
      deletedCount += await CleanupDirectoryAsync(
          l2Directory, catalogFilePaths, cancellationToken);
    }

    // Clean temp files
    deletedCount += CleanupTempFiles(l1Directory, l2Directory);

    _logger.LogInformation("Garbage collection complete: {Count} orphaned files deleted", deletedCount);

    return deletedCount;
  }

  /// <summary>
  /// Finds orphaned files without deleting them.
  /// </summary>
  /// <param name="catalog">The current catalog state.</param>
  /// <param name="l1Directory">The L1 Parquet directory.</param>
  /// <param name="l2Directory">The L2 Parquet directory.</param>
  /// <returns>List of orphaned file paths.</returns>
  public IReadOnlyList<string> FindOrphanedFiles(
      StreamCatalog catalog,
      string l1Directory,
      string l2Directory)
  {
    var catalogFilePaths = catalog.Entries
        .Select(e => Path.GetFullPath(e.FilePath))
        .ToHashSet(StringComparer.OrdinalIgnoreCase);

    var orphanedFiles = new List<string>();

    if (Directory.Exists(l1Directory)) {
      orphanedFiles.AddRange(FindOrphanedInDirectory(l1Directory, catalogFilePaths));
    }

    if (Directory.Exists(l2Directory)) {
      orphanedFiles.AddRange(FindOrphanedInDirectory(l2Directory, catalogFilePaths));
    }

    return orphanedFiles;
  }

  /// <summary>
  /// Cleans up orphaned files in a directory.
  /// </summary>
  private async Task<int> CleanupDirectoryAsync(
      string directory,
      HashSet<string> catalogFilePaths,
      CancellationToken cancellationToken)
  {
    var files = Directory.GetFiles(directory, "*.parquet", SearchOption.AllDirectories);
    var deletedCount = 0;

    foreach (var file in files) {
      cancellationToken.ThrowIfCancellationRequested();

      var fullPath = Path.GetFullPath(file);
      if (!catalogFilePaths.Contains(fullPath)) {
        try {
          await Task.Run(() => File.Delete(file), cancellationToken);
          _logger.LogInformation("Deleted orphaned file: {Path}", file);
          deletedCount++;
        } catch (Exception ex) {
          _logger.LogWarning(ex, "Failed to delete orphaned file: {Path}", file);
        }
      }
    }

    return deletedCount;
  }

  /// <summary>
  /// Finds orphaned files in a directory.
  /// </summary>
  private static IEnumerable<string> FindOrphanedInDirectory(
      string directory,
      HashSet<string> catalogFilePaths)
  {
    var files = Directory.GetFiles(directory, "*.parquet", SearchOption.AllDirectories);

    foreach (var file in files) {
      var fullPath = Path.GetFullPath(file);
      if (!catalogFilePaths.Contains(fullPath)) {
        yield return file;
      }
    }
  }

  /// <summary>
  /// Cleans up temporary files from both directories.
  /// </summary>
  private int CleanupTempFiles(string l1Directory, string l2Directory)
  {
    var deletedCount = 0;

    // Clean .tmp files in L1
    if (Directory.Exists(l1Directory)) {
      deletedCount += CleanupTempFilesInDirectory(l1Directory);
    }

    // Clean .tmp files in L2
    if (Directory.Exists(l2Directory)) {
      deletedCount += CleanupTempFilesInDirectory(l2Directory);
    }

    return deletedCount;
  }

  /// <summary>
  /// Cleans up temporary files in a single directory.
  /// </summary>
  private int CleanupTempFilesInDirectory(string directory)
  {
    var deletedCount = 0;

    try {
      var tmpFiles = Directory.GetFiles(directory, "*.tmp", SearchOption.AllDirectories);

      foreach (var tmpFile in tmpFiles) {
        try {
          File.Delete(tmpFile);
          _logger.LogDebug("Deleted temp file: {Path}", tmpFile);
          deletedCount++;
        } catch (Exception ex) {
          _logger.LogWarning(ex, "Failed to delete temp file: {Path}", tmpFile);
        }
      }
    } catch (Exception ex) {
      _logger.LogWarning(ex, "Error scanning for temp files in {Directory}", directory);
    }

    return deletedCount;
  }
}