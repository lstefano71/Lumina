using Lumina.Core.Configuration;
using Lumina.Core.Models;
using Lumina.Query;
using Lumina.Storage.Parquet;

namespace Lumina.Storage.Compaction;

/// <summary>
/// L2 Compactor consolidates daily L1 Parquet files into compressed L2 files.
/// Uses ZSTD compression for long-term storage efficiency.
/// </summary>
public sealed class L2Compactor
{
  private readonly CompactionSettings _settings;
  private readonly ParquetManager _parquetManager;
  private readonly ILogger<L2Compactor> _logger;

  public L2Compactor(
      CompactionSettings settings,
      ParquetManager parquetManager,
      ILogger<L2Compactor> logger)
  {
    _settings = settings;
    _parquetManager = parquetManager;
    _logger = logger;
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
    var outputPath = Path.Combine(outputDir, outputFileName);

    // Write consolidated file
    await ParquetWriter.WriteBatchAsync(entries, outputPath, _settings.MaxDynamicKeys, cancellationToken);

    _logger.LogInformation(
        "Consolidated {Count} entries from {Files} files into {Output}",
        entries.Count, l1Files.Count, outputFileName);

    // Delete old L1 files
    foreach (var file in l1Files) {
      try {
        File.Delete(file);
        _logger.LogDebug("Deleted L1 file: {File}", file);
      } catch (Exception ex) {
        _logger.LogWarning(ex, "Failed to delete L1 file: {File}", file);
      }
    }
  }

  /// <summary>
  /// Parses file info from a Parquet file path.
  /// </summary>
  private static FileInfo? ParseFileInfo(string filePath)
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

    return new FileInfo { Stream = stream, Date = date.Date };
  }

  private sealed class FileInfo
  {
    public required string Stream { get; init; }
    public required DateTime Date { get; init; }
  }
}