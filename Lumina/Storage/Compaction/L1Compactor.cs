using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Storage.Catalog;
using Lumina.Storage.Parquet;
using Lumina.Storage.Wal;

namespace Lumina.Storage.Compaction;

/// <summary>
/// L1 Compactor converts WAL segments to Parquet files.
/// Runs on a 10-minute window for time-based compaction.
/// </summary>
public sealed class L1Compactor
{
  private readonly WalManager _walManager;
  private readonly CursorManager _cursorManager;
  private readonly CompactionSettings _settings;
  private readonly CatalogManager? _catalogManager;
  private readonly ILogger<L1Compactor> _logger;

  public L1Compactor(
      WalManager walManager,
      CursorManager cursorManager,
      CompactionSettings settings,
      ILogger<L1Compactor> logger,
      CatalogManager? catalogManager = null)
  {
    _walManager = walManager;
    _cursorManager = cursorManager;
    _settings = settings;
    _logger = logger;
    _catalogManager = catalogManager;
  }

  /// <summary>
  /// Runs compaction for all streams.
  /// </summary>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The number of entries compacted.</returns>
  public async Task<int> CompactAllAsync(CancellationToken cancellationToken = default)
  {
    var totalEntries = 0;

    foreach (var stream in _walManager.GetActiveStreams()) {
      var entries = await CompactStreamAsync(stream, cancellationToken);
      totalEntries += entries;
    }

    return totalEntries;
  }

  /// <summary>
  /// Runs compaction for a specific stream.
  /// </summary>
  /// <param name="stream">The stream to compact.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The number of entries compacted.</returns>
  public async Task<int> CompactStreamAsync(string stream, CancellationToken cancellationToken = default)
  {
    var cursor = _cursorManager.GetCursor(stream);
    var entries = new List<LogEntry>();
    var lastOffset = cursor.LastCompactedOffset;
    var lastWalFile = cursor.LastCompactedWalFile;
    var currentLastWalFile = lastWalFile;
    var currentLastOffset = lastOffset;
    long? currentWalFileSize = null;

    // Read entries from WAL since last compaction
    var walFiles = _walManager.GetWalFiles(stream);
    foreach (var walFile in walFiles) {
      if (lastWalFile != null) {
        int cmp = string.Compare(walFile, lastWalFile, StringComparison.Ordinal);
        if (cmp < 0) {
          // File is older than the cursor's last compacted file, skip it entirely
          continue;
        }
      }

      using var reader = await _walManager.GetReaderAsync(walFile, stream, cancellationToken);
      await foreach (var walEntry in reader.ReadEntriesAsync(cancellationToken)) {
        if (lastWalFile != null && walFile == lastWalFile && walEntry.Offset <= cursor.LastCompactedOffset) {
          // Skip already compacted entries in the cursor's file
          continue;
        }

        walEntry.LogEntry.Offset = walEntry.Offset;
        entries.Add(walEntry.LogEntry);

        currentLastWalFile = walFile;
        currentLastOffset = walEntry.Offset;
      }

      // Track the file size for the current WAL file
      if (walFile == currentLastWalFile) {
        currentWalFileSize = reader.FileSize;
      }
    }

    if (entries.Count == 0) {
      _logger.LogDebug("No entries to compact for stream {Stream}", stream);
      return 0;
    }

    // Check if we have enough entries or time has passed
    var shouldCompact = ShouldCompact(entries);

    if (!shouldCompact) {
      _logger.LogDebug(
          "Skipping compaction for stream {Stream}: {Count} entries, waiting for threshold",
          stream, entries.Count);
      return 0;
    }

    // Generate output file path
    var outputDir = Path.Combine(_settings.L1Directory, stream);
    var startTime = entries.Min(e => e.Timestamp);
    var endTime = entries.Max(e => e.Timestamp);
    var outputFileName = ParquetWriter.GenerateFileName(stream, startTime, endTime);
    var outputPath = Path.GetFullPath(Path.Combine(outputDir, outputFileName));

    try {
      // Write Parquet file
      await ParquetWriter.WriteBatchAsync(entries, outputPath, _settings.MaxDynamicKeys, cancellationToken);

      // Get file info for catalog registration
      var fileInfo = new FileInfo(outputPath);

      // Register file in catalog (before cursor update for atomicity)
      if (_catalogManager != null) {
        var catalogEntry = new CatalogEntry {
          StreamName = stream,
          MinTime = startTime,
          MaxTime = endTime,
          FilePath = outputPath,
          Level = StorageLevel.L1,
          RowCount = entries.Count,
          FileSizeBytes = fileInfo.Length,
          AddedAt = DateTime.UtcNow,
          CompactionTier = 1
        };
        await _catalogManager.AddFileAsync(catalogEntry, cancellationToken);
      }

      // Update cursor with validation metadata
      _cursorManager.MarkCompactionComplete(
          stream,
          currentLastWalFile!,
          currentLastOffset,
          outputPath,
          walFileSize: currentWalFileSize,
          parquetEntryCount: entries.Count);

      // If the active WAL file was included in this compaction, rotate it now so it
      // becomes a sealed file eligible for deletion. It is intentionally kept alive
      // for one more cycle because entries may have been appended to it after the
      // compaction read (between currentLastOffset and the rotation point). Those
      // entries will be picked up and the file deleted on the next compaction run.
      var activeFilePath = _walManager.GetActiveWriterFilePath(stream);
      string? deferredDeletePath = null;
      if (string.Equals(currentLastWalFile, activeFilePath, StringComparison.OrdinalIgnoreCase)) {
        deferredDeletePath = activeFilePath;
        await _walManager.ForceRotateAsync(stream, cancellationToken);
        activeFilePath = _walManager.GetActiveWriterFilePath(stream);
        _logger.LogDebug("Rotated active WAL file {File} after compaction", Path.GetFileName(deferredDeletePath));
      }

      // Delete all sealed WAL files (non-active, non-deferred) — they are fully represented in Parquet
      foreach (var walFile in _walManager.GetWalFiles(stream)) {
        if (string.Equals(walFile, activeFilePath, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(walFile, deferredDeletePath, StringComparison.OrdinalIgnoreCase)) {
          continue;
        }

        if (_walManager.DeleteWalFile(walFile)) {
          _logger.LogDebug("Deleted compacted WAL file {File}", Path.GetFileName(walFile));
        }
      }

      _logger.LogInformation(
          "Compacted {Count} entries for stream {Stream} to {File}",
          entries.Count, stream, outputFileName);

      return entries.Count;
    } catch (Exception ex) {
      _logger.LogError(
          ex, "Failed to compact stream {Stream}", stream);

      // Clean up partial file if it exists
      if (File.Exists(outputPath)) {
        File.Delete(outputPath);
      }

      throw;
    }
  }

  /// <summary>
  /// Determines if compaction should run based on thresholds.
  /// </summary>
  private bool ShouldCompact(IReadOnlyList<LogEntry> entries)
  {
    if (entries.Count >= _settings.MaxEntriesPerFile) {
      return true;
    }

    var oldestTimestamp = entries.Min(e => e.Timestamp);
    var age = DateTime.UtcNow - oldestTimestamp;

    if (age >= _settings.L1Window) {
      return true;
    }

    return false;
  }

  /// <summary>
  /// Gets the list of L1 Parquet files for a stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <returns>List of Parquet file paths.</returns>
  public IReadOnlyList<string> GetL1Files(string stream)
  {
    var dir = Path.Combine(_settings.L1Directory, stream);

    if (!Directory.Exists(dir)) {
      return Array.Empty<string>();
    }

    return Directory.GetFiles(dir, "*.parquet")
        .OrderBy(f => f)
        .ToList();
  }

  /// <summary>
  /// Gets all L1 Parquet files across all streams.
  /// </summary>
  /// <returns>List of Parquet file paths.</returns>
  public IReadOnlyList<string> GetAllL1Files()
  {
    if (!Directory.Exists(_settings.L1Directory)) {
      return Array.Empty<string>();
    }

    return Directory.GetFiles(_settings.L1Directory, "*.parquet", SearchOption.AllDirectories)
        .OrderBy(f => f)
        .ToList();
  }
}