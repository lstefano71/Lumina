using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Query;
using Lumina.Storage.Catalog;
using Lumina.Storage.Parquet;

namespace Lumina.Storage.Compaction;

/// <summary>
/// L2 Compactor consolidates daily L1 Parquet files into compressed L2 files.
/// Uses ZSTD compression for long-term storage efficiency.
/// Implements atomic commit pattern to prevent duplicate rows during compaction.
/// Uses catalog time-range queries instead of filename parsing.
/// </summary>
public sealed class L2Compactor
{
  private readonly CompactionSettings _settings;
  private readonly ParquetManager _parquetManager;
  private readonly CatalogManager? _catalogManager;
  private readonly ILogger<L2Compactor> _logger;

  public L2Compactor(
      CompactionSettings settings,
      ParquetManager parquetManager,
      ILogger<L2Compactor> logger,
      CatalogManager? catalogManager = null)
  {
    _settings = settings;
    _parquetManager = parquetManager;
    _logger = logger;
    _catalogManager = catalogManager;
  }

  /// <summary>
  /// Runs L2 compaction for all streams.
  /// </summary>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The number of files consolidated.</returns>
  public async Task<int> CompactAllAsync(CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) {
      _logger.LogWarning("L2 compaction requires CatalogManager");
      return 0;
    }

    var streams = _catalogManager.GetStreams();
    var consolidatedCount = 0;

    foreach (var stream in streams) {
      if (cancellationToken.IsCancellationRequested) {
        break;
      }

      try {
        var count = await CompactStreamAsync(stream, cancellationToken);
        consolidatedCount += count;
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to compact stream {Stream}", stream);
      }
    }

    return consolidatedCount;
  }

  /// <summary>
  /// Runs L2 compaction for a specific stream.
  /// </summary>
  /// <param name="stream">The stream name.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The number of L1 files consolidated.</returns>
  public async Task<int> CompactStreamAsync(string stream, CancellationToken cancellationToken = default)
  {
    if (_catalogManager == null) {
      return 0;
    }

    // Get L1 entries eligible for daily compaction (files with MaxTime before cutoff)
    var cutoffDate = DateTime.UtcNow.Date;
    var eligibleEntries = _catalogManager.GetEligibleForDailyCompaction(stream, cutoffDate);

    if (eligibleEntries.Count == 0) {
      return 0;
    }

    // Group eligible entries by date for daily consolidation
    var groupsByDate = eligibleEntries
        .GroupBy(e => e.MaxTime.Date)
        .ToList();

    var consolidatedCount = 0;

    foreach (var group in groupsByDate) {
      if (cancellationToken.IsCancellationRequested) {
        break;
      }

      var entries = group.ToList();
      var date = group.Key;

      // Check if files are old enough (based on L2IntervalHours)
      var age = DateTime.UtcNow - date;
      if (age < TimeSpan.FromHours(_settings.L2IntervalHours)) {
        continue;
      }

      try {
        await ConsolidateDayAsync(
            stream,
            date,
            entries,
            cancellationToken);

        consolidatedCount += entries.Count;
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to consolidate {Stream} for {Date}",
            stream, date);
      }
    }

    return consolidatedCount;
  }

  /// <summary>
  /// Consolidates L1 files for a single day into an L2 file.
  /// Implements atomic commit: write L2 → update catalog → delete L1.
  /// </summary>
  private async Task ConsolidateDayAsync(
      string stream,
      DateTime date,
      IReadOnlyList<CatalogEntry> l1Entries,
      CancellationToken cancellationToken)
  {
    // Read all entries from L1 files
    var entries = new List<LogEntry>();
    var l1Files = l1Entries.Select(e => e.FilePath).ToList();

    foreach (var file in l1Files) {
      await foreach (var entry in ParquetReader.ReadEntriesAsync(file, cancellationToken)) {
        entries.Add(entry);
      }
    }

    if (entries.Count == 0) {
      _logger.LogDebug("No entries to consolidate for {Stream} on {Date}", stream, date);
      return;
    }

    // Calculate time bounds from actual data
    var minTime = entries.Min(e => e.Timestamp);
    var maxTime = entries.Max(e => e.Timestamp);

    // Generate L2 output path
    var outputDir = Path.Combine(_settings.L2Directory, stream);
    var outputFileName = $"{stream}_{date:yyyyMMdd}_consolidated.parquet";
    var outputPath = Path.GetFullPath(Path.Combine(outputDir, outputFileName));
    var tmpOutputPath = outputPath + ".tmp";

    // Write consolidated file to a temporary location
    await ParquetWriter.WriteBatchAsync(entries, tmpOutputPath, _settings.MaxDynamicKeys, cancellationToken);

    // Atomically move to final path
    File.Move(tmpOutputPath, outputPath, overwrite: true);

    // Get file info for catalog registration
    var fileInfo = new FileInfo(outputPath);

    // ATOMIC COMMIT: Update catalog before deleting L1 files
    // This prevents duplicate rows during the transition window
    if (_catalogManager != null) {
      var l2Entry = new CatalogEntry {
        StreamName = stream,
        MinTime = minTime,
        MaxTime = maxTime,
        FilePath = outputPath,
        Level = StorageLevel.L2,
        RowCount = entries.Count,
        FileSizeBytes = fileInfo.Length,
        AddedAt = DateTime.UtcNow,
        CompactionTier = 2
      };

      await _catalogManager.ReplaceFilesAsync(l1Files, l2Entry, cancellationToken);
      _logger.LogInformation(
          "Atomic commit: replaced {L1Count} L1 files with L2 file {Output} for {Stream}/{Date}",
          l1Files.Count, outputFileName, stream, date);
    }

    // Delete old L1 files AFTER catalog update
    foreach (var file in l1Files) {
      try {
        File.Delete(file);
        _logger.LogDebug("Deleted L1 file: {File}", file);
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to delete L1 file: {File}", file);
      }
    }

    _logger.LogInformation(
        "Consolidated {Count} entries from {Files} files into {Output}",
        entries.Count, l1Files.Count, outputFileName);
  }
}