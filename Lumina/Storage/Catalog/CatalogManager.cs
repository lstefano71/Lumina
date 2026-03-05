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
  private readonly object _catalogStateLock = new();
  private readonly JsonSerializerOptions _jsonOptions;

  private StreamCatalog _catalog;
  private readonly string _catalogPath;
  private readonly string _catalogTempPath;
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
            lock (_catalogStateLock) {
              _catalog = new StreamCatalog();
            }
          } else {
            throw;
          }
        }
      } else {
        _logger.LogInformation("No existing catalog found, starting with empty catalog");
        lock (_catalogStateLock) {
          _catalog = new StreamCatalog();
        }
      }

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
  public async Task AddFileAsync(CatalogEntry entry, CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(entry);

    await _lock.WaitAsync(cancellationToken);
    try {
      EnsureInitialized();

      lock (_catalogStateLock) {
        if (_catalog.Entries.Any(e => string.Equals(e.FilePath, entry.FilePath, StringComparison.OrdinalIgnoreCase))) {
          _logger.LogDebug("File {Path} already in catalog, skipping add", entry.FilePath);
          return;
        }

        _catalog.Entries.Add(entry);
        _catalog.LastModified = DateTime.UtcNow;
        _catalog.Version++;
      }

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

      int removed;
      lock (_catalogStateLock) {
        removed = 0;
        for (int i = _catalog.Entries.Count - 1; i >= 0; i--) {
          var entry = _catalog.Entries[i];
          if (oldFiles.Any(f => string.Equals(f, entry.FilePath, StringComparison.OrdinalIgnoreCase))) {
            _catalog.Entries.RemoveAt(i);
            removed++;
          }
        }

        _catalog.Entries.Add(newFile);
        _catalog.LastModified = DateTime.UtcNow;
        _catalog.Version++;
      }

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
  public async Task RemoveFileAsync(string filePath, CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(filePath);

    await _lock.WaitAsync(cancellationToken);
    try {
      EnsureInitialized();

      bool removed;
      lock (_catalogStateLock) {
        var index = _catalog.Entries.FindIndex(
            e => string.Equals(e.FilePath, filePath, StringComparison.OrdinalIgnoreCase));

        removed = index >= 0;
        if (removed) {
          _catalog.Entries.RemoveAt(index);
          _catalog.LastModified = DateTime.UtcNow;
          _catalog.Version++;
        }
      }

      if (removed) {
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
  public IReadOnlyList<CatalogEntry> GetEntries(string? stream = null, StorageLevel? level = null)
  {
    lock (_catalogStateLock) {
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
  }

  /// <summary>
  /// Gets all unique stream names.
  /// </summary>
  public IReadOnlyList<string> GetStreams()
  {
    lock (_catalogStateLock) {
      return _catalog.Entries
          .Select(e => e.StreamName)
          .Distinct(StringComparer.OrdinalIgnoreCase)
          .OrderBy(s => s)
          .ToList();
    }
  }

  /// <summary>
  /// Gets all files for a stream.
  /// </summary>
  public IReadOnlyList<string> GetFiles(string stream)
  {
    lock (_catalogStateLock) {
      return _catalog.Entries
          .Where(e => string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase))
          .Select(e => e.FilePath)
          .OrderBy(f => f)
          .ToList();
    }
  }

  /// <summary>
  /// Gets files that overlap with the specified time range.
  /// </summary>
  public IReadOnlyList<CatalogEntry> GetFilesInRange(string stream, DateTime start, DateTime end)
  {
    lock (_catalogStateLock) {
      return _catalog.Entries
          .Where(e => string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase))
          .Where(e => e.MinTime <= end && e.MaxTime >= start)
          .OrderBy(e => e.MinTime)
          .ToList();
    }
  }

  /// <summary>
  /// Gets all entries within a time range across all streams.
  /// </summary>
  public IReadOnlyList<CatalogEntry> GetEntriesByTimeRange(DateTime start, DateTime end, StorageLevel? level = null)
  {
    lock (_catalogStateLock) {
      var entries = _catalog.Entries
          .Where(e => e.MinTime <= end && e.MaxTime >= start);

      if (level.HasValue) {
        entries = entries.Where(e => e.Level == level.Value);
      }

      return entries.OrderBy(e => e.MinTime).ToList();
    }
  }

  /// <summary>
  /// Gets catalog entries for a stream matching the given storage level and compaction tier.
  /// </summary>
  public IReadOnlyList<CatalogEntry> GetEligibleEntries(
      string stream, StorageLevel level, int compactionTier)
  {
    lock (_catalogStateLock) {
      return _catalog.Entries
          .Where(e => string.Equals(e.StreamName, stream, StringComparison.OrdinalIgnoreCase))
          .Where(e => e.Level == level)
          .Where(e => e.CompactionTier == compactionTier)
          .OrderBy(e => e.MinTime)
          .ToList();
    }
  }

  /// <summary>
  /// Gets entries eligible for daily L2 compaction (L1 files with MaxTime before cutoff).
  /// </summary>
  public IReadOnlyList<CatalogEntry> GetEligibleForDailyCompaction(string stream, DateTime cutoffDate)
  {
    return GetEligibleEntries(stream, StorageLevel.L1, compactionTier: 1)
        .Where(e => e.MaxTime < cutoffDate)
        .ToList();
  }

  /// <summary>
  /// Gets all daily-tier L2 entries eligible for monthly consolidation.
  /// </summary>
  public IReadOnlyList<CatalogEntry> GetEligibleForMonthlyCompaction(string stream)
  {
    return GetEligibleEntries(stream, StorageLevel.L2, compactionTier: 2);
  }

  /// <summary>
  /// Gets entries eligible for monthly consolidation within a specific month.
  /// </summary>
  public IReadOnlyList<CatalogEntry> GetEligibleForMonthlyCompaction(string stream, int year, int month)
  {
    var monthStart = new DateTime(year, month, 1, 0, 0, 0, DateTimeKind.Utc);
    var monthEnd = monthStart.AddMonths(1);

    return GetEligibleEntries(stream, StorageLevel.L2, compactionTier: 2)
        .Where(e => e.MinTime >= monthStart && e.MaxTime < monthEnd)
        .ToList();
  }

  /// <summary>
  /// Gets the total size of all files in the catalog.
  /// </summary>
  public long GetTotalSize()
  {
    lock (_catalogStateLock) {
      return _catalog.Entries.Sum(e => e.FileSizeBytes);
    }
  }

  /// <summary>
  /// Gets the current catalog state (for recovery purposes).
  /// </summary>
  public StreamCatalog GetCatalogSnapshot()
  {
    lock (_catalogStateLock) {
      return new StreamCatalog {
        Entries = _catalog.Entries.ToList(),
        LastModified = _catalog.LastModified,
        Version = _catalog.Version
      };
    }
  }

  /// <summary>
  /// Reloads the catalog from an external state (used by rebuilder).
  /// </summary>
  public async Task ReloadFromStateAsync(StreamCatalog catalog, CancellationToken cancellationToken = default)
  {
    ArgumentNullException.ThrowIfNull(catalog);

    await _lock.WaitAsync(cancellationToken);
    try {
      lock (_catalogStateLock) {
        _catalog = new StreamCatalog {
          Entries = catalog.Entries.ToList(),
          LastModified = catalog.LastModified,
          Version = catalog.Version
        };
      }

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
    StreamCatalog snapshot;
    lock (_catalogStateLock) {
      snapshot = new StreamCatalog {
        Entries = _catalog.Entries.ToList(),
        LastModified = _catalog.LastModified,
        Version = _catalog.Version
      };
    }

    Directory.CreateDirectory(_options.CatalogDirectory);

    await using (var stream = new FileStream(
        _catalogTempPath,
        FileMode.Create,
        FileAccess.Write,
        FileShare.None)) {
      await JsonSerializer.SerializeAsync(stream, snapshot, _jsonOptions, cancellationToken);
      await stream.FlushAsync(cancellationToken);
    }

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

    lock (_catalogStateLock) {
      _catalog = catalog;
    }
  }

  /// <summary>
  /// Ensures the catalog has been initialized.
  /// </summary>
  private void EnsureInitialized()
  {
    if (!_initialized) {
      throw new InvalidOperationException("CatalogManager not initialized. Call InitializeAsync first.");
    }
  }

  /// <summary>
  /// Disposes the catalog manager.
  /// </summary>
  public void Dispose()
  {
    if (_disposed) {
      return;
    }

    _disposed = true;
    _lock.Dispose();
  }
}
