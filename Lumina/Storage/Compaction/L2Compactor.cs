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
    var l1Files = _parquetManager.GetL1Files();

    if (l1Files.Count == 0) {
      return 0;
    }

    // Group files by stream and date
    var groups = l1Files
        .Select(f => new { File = f, Info = ParseFileInfo(f) })
        .Where(x => x.Info != null)
        .GroupBy(x => new { x.Info!.Stream, x.Info.Date })
        .ToList();

    var consolidatedCount = 0;

    foreach (var group in groups) {
      if (cancellationToken.IsCancellationRequested) {
        break;
      }

      var files = group.Select(x => x.File).ToList();

      // Only consolidate if files are old enough
      var age = DateTime.UtcNow - group.Key.Date;
      if (age < TimeSpan.FromHours(_settings.L2IntervalHours)) {
        continue;
      }

      try {
        await ConsolidateDayAsync(
            group.Key.Stream,
            group.Key.Date,
            files,
            cancellationToken);

        consolidatedCount += files.Count;
      } catch (Exception ex) {
        _logger.LogError(ex, "Failed to consolidate {Stream} for {Date}",
            group.Key.Stream, group.Key.Date);
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
      IReadOnlyList<string> l1Files,
      CancellationToken cancellationToken)
  {
    // Read all entries from L1 files
    var entries = new List<LogEntry>();

    foreach (var file in l1Files) {
      await foreach (var entry in ParquetReader.ReadEntriesAsync(file, cancellationToken)) {
        entries.Add(entry);
      }
    }

    if (entries.Count == 0) {
      _logger.LogDebug("No entries to consolidate for {Stream} on {Date}", stream, date);
      return;
    }

    // Generate L2 output path
    var outputDir = Path.Combine(_settings.L2Directory, stream);
    var outputFileName = $"{stream}_{date:yyyyMMdd}_consolidated.parquet";
    var outputPath = Path.GetFullPath(Path.Combine(outputDir, outputFileName));

    // Write consolidated file
    await ParquetWriter.WriteBatchAsync(entries, outputPath, _settings.MaxDynamicKeys, cancellationToken);

    // Get file info for catalog registration
    var fileInfo = new FileInfo(outputPath);

    // ATOMIC COMMIT: Update catalog before deleting L1 files
    // This prevents duplicate rows during the transition window
    if (_catalogManager != null) {
      var l2Entry = new CatalogEntry {
        StreamName = stream,
        Date = date,
        FilePath = outputPath,
        Level = StorageLevel.L2,
        RowCount = entries.Count,
        FileSizeBytes = fileInfo.Length,
        AddedAt = DateTime.UtcNow
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

  /// <summary>
  /// Parses file info from a Parquet file path.
  /// </summary>
  private static ParsedFileInfo? ParseFileInfo(string filePath)
  {
    var fileName = Path.GetFileNameWithoutExtension(filePath);
    var parts = fileName.Split('_');

    if (parts.Length < 2) {
      return null;
    }

    // Format: stream_starttime_endtime.parquet
    var stream = parts[0];

    if (!DateTime.TryParseExact(parts[1], "yyyyMMdd", null,
        System.Globalization.DateTimeStyles.None, out var date)) {
      // Try parsing with time component
      if (!DateTime.TryParseExact(parts[1], "yyyyMMddHHmmss", null,
          System.Globalization.DateTimeStyles.None, out date)) {
        return null;
      }
    }

    return new ParsedFileInfo { Stream = stream, Date = date.Date };
  }

  private sealed class ParsedFileInfo
  {
    public required string Stream { get; init; }
    public required DateTime Date { get; init; }
  }
}