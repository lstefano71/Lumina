using Lumina.Core.Configuration;
using Lumina.Core.Models;
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
  private readonly ILogger<L1Compactor> _logger;

  public L1Compactor(
      WalManager walManager,
      CursorManager cursorManager,
      CompactionSettings settings,
      ILogger<L1Compactor> logger)
  {
    _walManager = walManager;
    _cursorManager = cursorManager;
    _settings = settings;
    _logger = logger;
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

      // Update cursor
      _cursorManager.MarkCompactionComplete(stream, currentLastWalFile!, currentLastOffset, outputPath);

      // Delete sealed WAL files — all non-active files are now fully represented in Parquet
      var activeFilePath = _walManager.GetActiveWriterFilePath(stream);
      foreach (var walFile in _walManager.GetWalFiles(stream)) {
        if (string.Equals(walFile, activeFilePath, StringComparison.OrdinalIgnoreCase)) {
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