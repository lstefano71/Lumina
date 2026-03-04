using System.Text.Json;

namespace Lumina.Storage.Catalog;

/// <summary>
/// Manages the stream catalog with atomic visibility guarantees.
/// Provides ACID-like semantics for file visibility transitions.
/// </summary>
public sealed class CatalogManager : IDisposable
{
  private readonly CatalogOptions _options;
  private readonly ILogger<CatalogManager> _logger;
  private readonly SemaphoreSlim _lock = new(1, 1);
  private readonly JsonSerializerOptions _jsonOptions;

  private StreamCatalog _catalog;
  private string _catalogPath;
  private string _catalogTempPath;
  private bool _initialized;
  private bool _disposed;

  public CatalogManager(CatalogOptions options, ILogger<CatalogManager> logger)
  {
    _options = options ?? throw new ArgumentNullException(nameof(options));
    _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    _catalog = new StreamCatalog();
    _catalogPath = Path.Combine(_options.CatalogDirectory, _options.CatalogFileName);
    _catalogTempPath = Path.Combine(_options.CatalogDirectory, _options.CatalogTempFileName);

    _jsonOptions = new JsonSerializerOptions {
      WriteIndented = true,
      PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
  }

  /// <summary>
  /// Gets the catalog options.
  /// </summary>
  public CatalogOptions Options => _options;

  /// <summary>
  /// Initializes the catalog by loading from disk or creating new.
  /// </summary>
  /// <param name="cancellationToken">Cancellation token.</param>
  public async Task InitializeAsync(CancellationToken cancellationToken = default)
  {
    await _lock.WaitAsync(cancellationToken);
    try {
      if (_initialized) {
        return;
      }

      Directory.CreateDirectory(_options.CatalogDirectory);

      if (File.Exists(_catalogPath)) {
        try {
          await LoadCatalogAsync(cancellationToken);
          _logger.LogInformation(
              "Loaded catalog with {Count} entries from {Path}",
              _catalog.Entries.Count, _catalogPath);
        } catch (Exception ex) {
          _logger.LogWarning(ex, "Failed to load catalog from {Path}", _catalogPath);

          if (_options.EnableAutoRebuild) {
            _logger.LogInformation("Auto-rebuild is enabled, catalog will be rebuilt");
            _catalog = new StreamCatalog();
          } else {
            throw;
          }
        }
      } else {
        _logger.LogInformation("No existing catalog found, starting with empty catalog");
        _catalog = new StreamCatalog();
      }

      // Clean up any leftover temp file
      if (File.Exists(_catalogTempPath)) {
        try {
          File.Delete(_catalogTempPath);
          _logger.LogDebug("Cleaned up leftover temp catalog file");
        } catch (Exception ex) {
          _logger.LogWarning(ex, "Failed to clean up temp catalog file");
        }
      }

      _initialized = true;
    } finally {
      _lock.Release();
    }
  }

  /// <summary>
  /// Adds a new file to the catalog atomically.
  /// </summary>
  /// <param name="entry">The catalog entry to add.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  public async Task AddFileAsync(CatalogEntry entry, CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(entry);

    await _lock.WaitAsync(cancellationToken);
    try {
      EnsureInitialized();

      // Check if file already exists
      if (_catalog.Entries.Any(e => string.Equals(e.FilePath, entry.FilePath, StringComparison.OrdinalIgnoreCase))) {
        _logger.LogDebug("File {Path} already in catalog, skipping add", entry.FilePath);
        return;
      }

      _catalog.Entries.Add(entry);
      _catalog.LastModified = DateTime.UtcNow;
      _catalog.Version++;

      await PersistAsync(cancellationToken);

      _logger.LogDebug(
          "Added file {Path} to catalog (stream={Stream}, level={Level}, rows={Rows})",
          entry.FilePath, entry.StreamName, entry.Level, entry.RowCount);
    } finally {
      _lock.Release();
    }
  }

  /// <summary>
  /// Atomically replaces multiple files with a single new file.
  /// Used for L2 compaction to prevent duplicate rows.
  /// </summary>
  /// <param name="oldFiles">The files to remove.</param>
  /// <param name="newFile">The new file to add.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  public async Task ReplaceFilesAsync(
      IReadOnlyList<string> oldFiles,
      CatalogEntry newFile,
      CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(oldFiles);
    ArgumentNullException.ThrowIfNull(newFile);

    await _lock.WaitAsync(cancellationToken);
    try {
      EnsureInitialized();

      // Remove old files
      var removed = 0;
      for (int i = _catalog.Entries.Count - 1; i >= 0; i--) {
        var entry = _catalog.Entries[i];
        if (oldFiles.Any(f => string.Equals(f, entry.FilePath, StringComparison.OrdinalIgnoreCase))) {
          _catalog.Entries.RemoveAt(i);
          removed++;
        }
      }

      // Add new file
      _catalog.Entries.Add(newFile);
      _catalog.LastModified = DateTime.UtcNow;
      _catalog.Version++;

      await PersistAsync(cancellationToken);

      _logger.LogInformation(
          "Atomically replaced {OldCount} files with {NewPath} (stream={Stream}, rows={Rows})",
          removed, newFile.FilePath, newFile.StreamName, newFile.RowCount);
    } finally {
      _lock.Release();
    }
  }

  /// <summary>
  /// Removes a file from the catalog.
  /// </summary>
  /// <param name="filePath">The file path to remove.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  public async Task RemoveFileAsync(string filePath, CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(filePath);

    await _lock.WaitAsync(cancellationToken);
    try {
      EnsureInitialized();

      var index = _catalog.Entries.FindIndex(
          e => string.Equals(e.FilePath, filePath, StringComparison.OrdinalIgnoreCase));

      if (index >= 0) {
        _catalog.Entries.RemoveAt(index);
        _catalog.LastModified = DateTime.UtcNow;
        _catalog.Version++;

        await PersistAsync(cancellationToken);

        _logger.LogDebug("Removed file {Path} from catalog", filePath);
      }
    } finally {
      _lock.Release();
    }
  }

  /// <summary>
  /// Gets catalog entries filtered by optional criteria.
  /// </summary>
  /// <param name="stream">Optional stream name filter.</param>
  /// <param name="level">Optional storage level filter.</param>
  /// <returns>List of matching entries.</returns>
  public IReadOnlyList<CatalogEntry> GetEntries(string? stream = null, StorageLevel? level = null)
  {
    var entries = _catalog.Entries.AsEnumerable();

    if (!string.IsNullOrEmpty(stream)) {
      entries = entries.Where(e =>
          string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase));
    }

    if (level.HasValue) {
      entries = entries.Where(e => e.Level == level.Value);
    }

    return entries.ToList();
  }

  /// <summary>
  /// Gets all unique stream names.
  /// </summary>
  /// <returns>List of stream names.</returns>
  public IReadOnlyList<string> GetStreams()
  {
    return _catalog.Entries
        .Select(e => e.StreamName)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .OrderBy(s => s)
        .ToList();
  }

  /// <summary>
  /// Gets all files for a stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <returns>List of file paths.</returns>
  public IReadOnlyList<string> GetFiles(string stream)
  {
    return _catalog.Entries
        .Where(e => string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase))
        .Select(e => e.FilePath)
        .OrderBy(f => f)
        .ToList();
  }

  /// <summary>
  /// Gets files that overlap with the specified time range.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="start">Start of the time range.</param>
  /// <param name="end">End of the time range.</param>
  /// <returns>List of catalog entries overlapping the time range.</returns>
  public IReadOnlyList<CatalogEntry> GetFilesInRange(string stream, DateTime start, DateTime end)
  {
    return _catalog.Entries
        .Where(e => string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase))
        .Where(e => e.MinTime <= end && e.MaxTime >= start)
        .OrderBy(e => e.MinTime)
        .ToList();
  }

  /// <summary>
  /// Gets all entries within a time range across all streams.
  /// </summary>
  /// <param name="start">Start of the time range.</param>
  /// <param name="end">End of the time range.</param>
  /// <param name="level">Optional storage level filter.</param>
  /// <returns>List of catalog entries overlapping the time range.</returns>
  public IReadOnlyList<CatalogEntry> GetEntriesByTimeRange(DateTime start, DateTime end, StorageLevel? level = null)
  {
    var entries = _catalog.Entries
        .Where(e => e.MinTime <= end && e.MaxTime >= start);

    if (level.HasValue) {
      entries = entries.Where(e => e.Level == level.Value);
    }

    return entries.OrderBy(e => e.MinTime).ToList();
  }

  /// <summary>
  /// Gets entries eligible for daily L2 compaction (L1 files with MaxTime before cutoff).
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cutoffDate">The cutoff date (files with MaxTime before this are eligible).</param>
  /// <returns>List of catalog entries eligible for compaction.</returns>
  public IReadOnlyList<CatalogEntry> GetEligibleForDailyCompaction(string stream, DateTime cutoffDate)
  {
    return _catalog.Entries
        .Where(e => string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase))
        .Where(e => e.Level == StorageLevel.L1)
        .Where(e => e.MaxTime < cutoffDate)
        .OrderBy(e => e.MinTime)
        .ToList();
  }

  /// <summary>
  /// Gets entries eligible for monthly consolidation.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="year">The year.</param>
  /// <param name="month">The month (1-12).</param>
  /// <returns>List of catalog entries within the specified month.</returns>
  public IReadOnlyList<CatalogEntry> GetEligibleForMonthlyCompaction(string stream, int year, int month)
  {
    var monthStart = new DateTime(year, month, 1, 0, 0, 0, DateTimeKind.Utc);
    var monthEnd = monthStart.AddMonths(1);

    return _catalog.Entries
        .Where(e => string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase))
        .Where(e => e.Level == StorageLevel.L2)
        .Where(e => e.MinTime >= monthStart && e.MaxTime < monthEnd)
        .OrderBy(e => e.MinTime)
        .ToList();
  }

  /// <summary>
  /// Gets the total size of all files in the catalog.
  /// </summary>
  /// <returns>Total size in bytes.</returns>
  public long GetTotalSize()
  {
    return _catalog.Entries.Sum(e => e.FileSizeBytes);
  }

  /// <summary>
  /// Gets the current catalog state (for recovery purposes).
  /// </summary>
  /// <returns>A deep copy of the current catalog.</returns>
  public StreamCatalog GetCatalogSnapshot()
  {
    return new StreamCatalog {
      Entries = _catalog.Entries.ToList(),
      LastModified = _catalog.LastModified,
      Version = _catalog.Version
    };
  }

  /// <summary>
  /// Reloads the catalog from an external state (used by rebuilder).
  /// </summary>
  /// <param name="catalog">The catalog state to load.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  public async Task ReloadFromStateAsync(StreamCatalog catalog, CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(catalog);

    await _lock.WaitAsync(cancellationToken);
    try {
      _catalog = new StreamCatalog {
        Entries = catalog.Entries.ToList(),
        LastModified = catalog.LastModified,
        Version = catalog.Version
      };

      await PersistAsync(cancellationToken);

      _logger.LogInformation(
          "Reloaded catalog from external state: {Count} entries, version {Version}",
          _catalog.Entries.Count, _catalog.Version);
    } finally {
      _lock.Release();
    }
  }

  /// <summary>
  /// Persists the catalog to disk using safe-write pattern.
  /// </summary>
  private async Task PersistAsync(CancellationToken cancellationToken)
  {
    // Ensure directory exists
    Directory.CreateDirectory(_options.CatalogDirectory);

    // Write to temp file first
    await using (var stream = new FileStream(
        _catalogTempPath,
        FileMode.Create,
        FileAccess.Write,
        FileShare.None)) {
      await JsonSerializer.SerializeAsync(stream, _catalog, _jsonOptions, cancellationToken);
      await stream.FlushAsync(cancellationToken);
    }

    // Atomic rename (on Windows, this is atomic when overwriting)
    File.Move(_catalogTempPath, _catalogPath, overwrite: true);
  }

  /// <summary>
  /// Loads the catalog from disk.
  /// </summary>
  private async Task LoadCatalogAsync(CancellationToken cancellationToken)
  {
    await using var stream = File.OpenRead(_catalogPath);
    var catalog = await JsonSerializer.DeserializeAsync<StreamCatalog>(stream, _jsonOptions, cancellationToken);

    if (catalog == null) {
      throw new InvalidOperationException("Failed to deserialize catalog: result was null");
    }

    _catalog = catalog;
  }

  /// <summary>
  /// Ensures the catalog has been initialized.
  /// </summary>
  private void EnsureInitialized()
  {
    if (!_initialized) {
      throw new InvalidOperationException("CatalogManager has not been initialized. Call InitializeAsync first.");
    }
  }

  public void Dispose()
  {
    if (!_disposed) {
      _lock.Dispose();
      _disposed = true;
    }
  }
}